package proxy

import (
	"context"
	"fmt"
	"fnysfd/internal/logger"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// PrefetchItem 单个预取任务描述
type PrefetchItem struct {
	ItemID        string // 飞牛 ItemId
	UserID        string // 用户 UserId（用于构造 PlaybackInfo 请求，可为空）
	Name          string // 媒体名称（仅用于日志展示）
	Type          string // 媒体类型（Movie/Episode/Series 等，仅用于日志展示）
	AlreadyProbed bool   // ✅ 飞牛是否已真正 probe（MediaStreams 中 Codec 非空）
}

// BatchStats 批量预取统计
type BatchStats struct {
	Total     int           // 总任务数
	Success   int           // 新预取成功数
	Skipped   int           // 跳过数（缓存已命中，无需预取）
	Deduped   int           // 去重跳过数（TTL 内已请求过）
	Failed    int           // 失败数（真正的请求失败）
	Cancelled int           // 引擎停止导致取消
	Duration  time.Duration // 总耗时
}

// String 返回统计的可读字符串（用于日志输出）
func (s BatchStats) String() string {
	return fmt.Sprintf("总计=%d 成功=%d 跳过=%d 去重=%d 失败=%d 取消=%d 耗时=%v",
		s.Total, s.Success, s.Skipped, s.Deduped, s.Failed, s.Cancelled, s.Duration)
}

// BatchPrefetcher 批量预取引擎
type BatchPrefetcher struct {
	server      *Server
	logger      *logger.Logger
	sem         chan struct{}  // 信号量控制并发
	concurrency int            // 当前并发数
	stopCh      chan struct{}  // 停止信号
	wg          sync.WaitGroup // 等待在途任务完成
	semMu       sync.RWMutex   // 保护 sem / concurrency 的并发读写（动态调整并发时使用）

	// 保护 stop + wg.Add 的原子性
	stopMu  sync.Mutex
	stopped atomic.Bool
}

// NewBatchPrefetcher 创建批量预取引擎
func NewBatchPrefetcher(s *Server, concurrency int) *BatchPrefetcher {
	if concurrency <= 0 {
		concurrency = 1
	}
	return &BatchPrefetcher{
		server:      s,
		logger:      s.logger,
		sem:         make(chan struct{}, concurrency),
		concurrency: concurrency,
		stopCh:      make(chan struct{}),
	}
}

// UpdateConcurrency 动态调整并发数（重建信号量 channel）
//
// 重建后新的并发限制对后续获取信号量的任务立即生效；已在飞的旧任务仍按旧
// channel 释放令牌，过渡期内并发可能短暂超出新上限（best-effort，不阻塞调用）。
func (bp *BatchPrefetcher) UpdateConcurrency(n int) {
	if n <= 0 {
		n = 1
	}
	bp.semMu.Lock()
	defer bp.semMu.Unlock()
	if bp.concurrency == n {
		return
	}
	bp.sem = make(chan struct{}, n)
	bp.concurrency = n
	bp.logger.Info("⚙️ [批量预取] 并发数已调整为: %d", n)
}

// Stop 停止引擎：关闭 stopCh 并等待所有在途任务完成
//
// 关键：stopMu 保护 "判断 stopped + close(stopCh)"，与 PrefetchBatch 中的
// "判断 stopped + wg.Add" 形成互斥，避免 wg.Add 在 wg.Wait 之后执行的竞态。
func (bp *BatchPrefetcher) Stop() {
	bp.stopMu.Lock()
	if bp.stopped.Swap(true) {
		bp.stopMu.Unlock()
		return
	}
	close(bp.stopCh)
	bp.stopMu.Unlock()

	bp.wg.Wait()
}

// PrefetchItem 预取单个媒体项的 PlaybackInfo
//
// 返回值：
//   - hit:     缓存已命中（Skipped）
//   - deduped: TTL 内已请求过（Deduped，不计失败）
//   - ok:      本次新预取成功（Success）
//
// 三者互斥，全 false 时为真正的请求失败（Failed）。
//
// 处理流程：缓存命中检查 → 去重检查 → 信号量限流 → 单次请求 → Handle 处理。
// 单次请求不重试（批量场景由调用方控制重试/范围），失败返回 ok=false。
// 请求头设置 X-Fnysfd-Next-Prefetch: 1，避免 Handle 内部"下一集预取"回调
// 重复触发（海报墙/全库扫描自行控制预取范围）。
func (bp *BatchPrefetcher) PrefetchItem(ctx context.Context, itemID, userID string, authHeaders http.Header, prefix string, dedupTTL int64) (hit bool, deduped bool, ok bool) {
	if itemID == "" {
		return false, false, false
	}

	// 1. 缓存命中检查：MediaSource 已缓存且直链已解析，直接返回命中
	if source, found := bp.server.cache.GetByItemID(itemID); found {
		if _, urlFound := bp.server.cache.GetStreamURL(source.ID); urlFound {
			bp.logger.Debug("⏭️ [批量预取] 跳过（已缓存且直链已解析）: ItemId=%s", itemID)
			return true, false, false
		}
	}

	// 2. 去重检查：TTL 内已请求过则跳过（shouldSkipPrefetch 内部会写入去重标记）
	dedupKey := buildPrefetchKey(prefix, itemID, "")
	if bp.server.shouldSkipPrefetch(dedupKey, dedupTTL, "批量预取") {
		return false, true, false
	}

	// 3. 信号量限流：获取令牌或响应 ctx 取消 / 引擎停止
	bp.semMu.RLock()
	sem := bp.sem
	bp.semMu.RUnlock()
	select {
	case sem <- struct{}{}:
		defer func() { <-sem }()
	case <-ctx.Done():
		return false, false, false
	case <-bp.stopCh:
		return false, false, false
	}

	// 4. 构造 PlaybackInfo 请求（复用项目标准 URL 与 header 处理模式）
	//    targetURL 通过 proxyMu 读锁保护读取（与 Reload 写入互斥）
	bp.server.proxyMu.RLock()
	targetURL := bp.server.targetURL
	bp.server.proxyMu.RUnlock()

	fullURL := targetURL.Scheme + "://" + targetURL.Host + "/emby/Items/" + itemID + "/PlaybackInfo"
	if userID != "" {
		fullURL += "?UserId=" + userID
	}

	// 请求头处理：克隆 header（nil 时用空 header）→ 删除 Accept-Encoding → 设置 Accept: application/json
	reqHeaders := http.Header{}
	if authHeaders != nil {
		reqHeaders = authHeaders.Clone()
	}
	reqHeaders.Del("Accept-Encoding")            // 让 Transport 自动处理 gzip，避免压缩响应导致解析失败
	reqHeaders.Set("Accept", "application/json") // 强制 JSON 响应，避免飞牛返回 HTML
	// 标记为批量预取请求：Handle 内部的"下一集预取"回调检测到此头会跳过，
	// 避免批量预取逐集递归触发（海报墙/全库扫描自行控制预取范围）
	reqHeaders.Set("X-Fnysfd-Next-Prefetch", "1")

	req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
	if err != nil {
		bp.logger.Warn("❌ [批量预取] 创建请求失败: ItemId=%s err=%v", itemID, err)
		return false, false, false
	}
	req.Header = reqHeaders
	req.Host = targetURL.Host

	// 5. 单次请求（不重试），失败直接返回
	//    ✅ 用 prefetchClient（长超时）：飞牛"实时 probe 未探测的媒体"需要几十秒
	resp, err := bp.server.prefetchClient.Do(req)
	if err != nil {
		bp.logger.Debug("⚠️ [批量预取] 请求失败: ItemId=%s err=%v", itemID, err)
		return false, false, false
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		bp.logger.Debug("⚠️ [批量预取] 非200响应: ItemId=%s status=%d", itemID, resp.StatusCode)
		return false, false, false
	}

	// 6. 读取响应体（限制 10MB，防止恶意大响应导致 OOM）
	body, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
	resp.Body.Close()
	if err != nil {
		bp.logger.Warn("⚠️ [批量预取] 读取响应体失败: ItemId=%s err=%v", itemID, err)
		return false, false, false
	}
	if len(body) == 0 {
		bp.logger.Debug("⚠️ [批量预取] 响应体为空: ItemId=%s", itemID)
		return false, false, false
	}

	// 7. 交给 PlaybackHandler 处理（自动完成 STRM 缓存 + 预加载 + 回调）
	//    X-Fnysfd-Next-Prefetch 头已设置，下一集预取回调会被跳过
	_, _ = bp.server.playbackHandler.Handle(resp, body)
	bp.logger.Debug("✅ [批量预取] 成功: ItemId=%s", itemID)
	return false, false, true
}

// PrefetchBatch 批量并发预取
//
// authHeaders 为从 AuthStore 获取的认证头（X-Emby-Token / Authorization），
// 传给每个 PrefetchItem 用于构造带认证的 PlaybackInfo 请求。
// 通过信号量控制总并发，返回汇总统计。
//
// 统计规则（基于 PrefetchItem 返回值）：
//   - hit=true      → Skipped（缓存已命中，无需预取）
//   - deduped=true  → Deduped（TTL 内已请求过）
//   - ok=true       → Success（本次新预取成功）
//   - 其余          → Failed（真正的请求失败）
//
// 引擎停止（Stop）后调用本方法会立即返回，全部计入 Cancelled。
func (bp *BatchPrefetcher) PrefetchBatch(ctx context.Context, items []PrefetchItem, authHeaders http.Header, prefix string, dedupTTL int64, logTag string) BatchStats {
	start := time.Now()
	stats := BatchStats{Total: len(items)}

	if len(items) == 0 {
		return stats
	}

	if logTag == "" {
		logTag = "批量预取"
	}

	// 原子：判断 stopped + wg.Add（避免与 Stop 的 wg.Wait 竞态）
	bp.stopMu.Lock()
	if bp.stopped.Load() {
		bp.stopMu.Unlock()
		stats.Cancelled = len(items)
		stats.Duration = time.Since(start)
		bp.logger.Info("📊 [%s] 批量预取跳过（引擎已停止）: %s", logTag, stats.String())
		return stats
	}
	bp.wg.Add(len(items))
	bp.stopMu.Unlock()

	// 派生受 stopCh 控制的 ctx：Stop() 关闭 stopCh 后，批量内所有在途请求尽快退出
	batchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-bp.stopCh:
			cancel()
		case <-batchCtx.Done():
		}
	}()

	var mu sync.Mutex
	var localWg sync.WaitGroup
	success, skipped, deduped, failed := 0, 0, 0, 0

	// localWg 用于本批次统计收集；bp.wg 用于 Stop() 等待在途任务
	localWg.Add(len(items))
	for i := range items {
		item := items[i] // Go 1.21 循环变量需显式拷贝
		go func() {
			defer localWg.Done()
			defer bp.wg.Done()

			// 引擎已停止则直接计入失败
			select {
			case <-bp.stopCh:
				mu.Lock()
				failed++
				mu.Unlock()
				return
			default:
			}

			hit, dedup, ok := bp.PrefetchItem(batchCtx, item.ItemID, item.UserID, authHeaders, prefix, dedupTTL)

			mu.Lock()
			switch {
			case hit:
				skipped++ // 缓存已命中
			case dedup:
				deduped++ // TTL 内已请求过
			case ok:
				success++ // 新预取成功
			default:
				failed++ // 真正的请求失败
			}
			mu.Unlock()
		}()
	}
	localWg.Wait()

	stats.Success = success
	stats.Skipped = skipped
	stats.Deduped = deduped
	stats.Failed = failed
	stats.Duration = time.Since(start)

	bp.logger.Info("📊 [%s] 批量预取完成: %s", logTag, stats.String())
	return stats
}
