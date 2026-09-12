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
	ItemID string // 飞牛 ItemId
	UserID string // 用户 UserId（用于构造 PlaybackInfo 请求，可为空）
	Name   string // 媒体名称（仅用于日志展示）
	Type   string // 媒体类型（Movie/Episode/Series 等，仅用于日志展示）
}

// BatchStats 批量预取统计
type BatchStats struct {
	Total     int
	Success   int
	Skipped   int
	Deduped   int // 去重跳过（TTL 内已请求过）
	Failed    int // 真正的请求失败
	Cancelled int // 引擎停止导致取消
	Duration  time.Duration
}

func (s BatchStats) String() string {
	return fmt.Sprintf("总计=%d 成功=%d 跳过=%d 去重=%d 失败=%d 取消=%d 耗时=%v",
		s.Total, s.Success, s.Skipped, s.Deduped, s.Failed, s.Cancelled, s.Duration)
}

// BatchPrefetcher 批量预取引擎
type BatchPrefetcher struct {
	server      *Server
	logger      *logger.Logger
	sem         chan struct{}
	concurrency int
	stopCh      chan struct{}
	wg          sync.WaitGroup
	semMu       sync.RWMutex

	// 新增：保护 stop + wg.Add 的原子性
	stopMu  sync.Mutex
	stopped atomic.Bool
}

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

// PrefetchItem 预取单个媒体项
//
// 返回值：
//   - hit:     缓存已命中（Skipped）
//   - deduped: TTL 内已请求过（Deduped，不计失败）
//   - ok:      本次新预取成功（Success）
//
// 三者互斥，全 false 时为真正的请求失败（Failed）。
func (bp *BatchPrefetcher) PrefetchItem(ctx context.Context, itemID, userID string, authHeaders http.Header, prefix string, dedupTTL int64) (hit bool, deduped bool, ok bool) {
	if itemID == "" {
		return false, false, false
	}

	// 1. 缓存命中
	if source, found := bp.server.cache.GetByItemID(itemID); found {
		if _, urlFound := bp.server.cache.GetStreamURL(source.ID); urlFound {
			bp.logger.Debug("⏭️ [批量预取] 跳过（已缓存且直链已解析）: ItemId=%s", itemID)
			return true, false, false
		}
	}

	// 2. 去重检查
	dedupKey := buildPrefetchKey(prefix, itemID, "")
	if bp.server.shouldSkipPrefetch(dedupKey, dedupTTL, "批量预取") {
		return false, true, false
	}

	// 3. 信号量
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

	// 4. 构造请求
	bp.server.proxyMu.RLock()
	targetURL := bp.server.targetURL
	bp.server.proxyMu.RUnlock()

	fullURL := targetURL.Scheme + "://" + targetURL.Host + "/emby/Items/" + itemID + "/PlaybackInfo"
	if userID != "" {
		fullURL += "?UserId=" + userID
	}

	reqHeaders := http.Header{}
	if authHeaders != nil {
		reqHeaders = authHeaders.Clone()
	}
	reqHeaders.Del("Accept-Encoding")
	reqHeaders.Set("Accept", "application/json")
	reqHeaders.Set("X-Fnysfd-Next-Prefetch", "1")

	req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
	if err != nil {
		bp.logger.Warn("❌ [批量预取] 创建请求失败: ItemId=%s err=%v", itemID, err)
		return false, false, false
	}
	req.Header = reqHeaders
	req.Host = targetURL.Host

	// 5. 单次请求
	resp, err := bp.server.retryClient.Do(req)
	if err != nil {
		bp.logger.Debug("⚠️ [批量预取] 请求失败: ItemId=%s err=%v", itemID, err)
		return false, false, false
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		bp.logger.Debug("⚠️ [批量预取] 非200响应: ItemId=%s status=%d", itemID, resp.StatusCode)
		return false, false, false
	}

	// 6. 读响应
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

	// 7. 交给 Handle
	_, _ = bp.server.playbackHandler.Handle(resp, body)
	bp.logger.Debug("✅ [批量预取] 成功: ItemId=%s", itemID)
	return false, false, true
}

// PrefetchBatch 批量并发预取
func (bp *BatchPrefetcher) PrefetchBatch(ctx context.Context, items []PrefetchItem, authHeaders http.Header, prefix string, dedupTTL int64, logTag string) BatchStats {
	start := time.Now()
	stats := BatchStats{Total: len(items)}

	if len(items) == 0 {
		return stats
	}
	if logTag == "" {
		logTag = "批量预取"
	}

	// 原子：判断 stopped + wg.Add
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

	localWg.Add(len(items))
	for i := range items {
		item := items[i]
		go func() {
			defer localWg.Done()
			defer bp.wg.Done()

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
				skipped++
			case dedup:
				deduped++
			case ok:
				success++
			default:
				failed++
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
