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
	ItemID         string // 飞牛 ItemId
	UserID         string // 用户 UserId
	Name           string // 媒体名称
	Type           string // 媒体类型（Movie/Series/Episode/Video）
	AlreadyProbed  bool   // 飞牛是否已 probe

	// ✅ 新增（用于豆瓣抓取）
	ImdbID         string // IMDB ID（优先用）
	ProductionYear int    // 上映年份（辅助匹配）
	SeriesID       string // 所属剧集 ID（Episode/Season 用）
	SeriesName     string // 所属剧集名（季评分用）
	SeasonNumber   int    // 季号（季评分用）
}

// BatchStats 批量预取统计
type BatchStats struct {
	Total     int
	Success   int
	Skipped   int
	Deduped   int
	Failed    int
	Cancelled int
	Duration  time.Duration
}

func (s BatchStats) String() string {
	return fmt.Sprintf("总计=%d 成功=%d 跳过=%d 去重=%d 失败=%d 取消=%d 耗时=%v",
		s.Total, s.Success, s.Skipped, s.Deduped, s.Failed, s.Cancelled, s.Duration)
}

type BatchPrefetcher struct {
	server      *Server
	logger      *logger.Logger
	sem         chan struct{}
	concurrency int
	stopCh      chan struct{}
	wg          sync.WaitGroup
	semMu       sync.RWMutex
	stopMu      sync.Mutex
	stopped     atomic.Bool
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
// ⚠️ 09-15 版本：GET，URL 带 UserId，用 retryClient
func (bp *BatchPrefetcher) PrefetchItem(ctx context.Context, itemID, userID string, authHeaders http.Header, prefix string, dedupTTL int64) (hit bool, deduped bool, ok bool) {
	if itemID == "" {
		return false, false, false
	}

	if source, found := bp.server.cache.GetByItemID(itemID); found {
		if _, urlFound := bp.server.cache.GetStreamURL(source.ID); urlFound {
			bp.logger.Debug("⏭️ [批量预取] 跳过（已缓存且直链已解析）: ItemId=%s", itemID)
			return true, false, false
		}
	}

	dedupKey := buildPrefetchKey(prefix, itemID, "")
	if bp.server.shouldSkipPrefetch(dedupKey, dedupTTL, "批量预取") {
		return false, true, false
	}

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

	bp.server.proxyMu.RLock()
	targetURL := bp.server.targetURL
	bp.server.proxyMu.RUnlock()

	fullURL := targetURL.Scheme + "://" + targetURL.Host + "/emby/Items/" + itemID + "/PlaybackInfo"
	if userID != "" {
		fullURL += "?UserId=" + userID
	}

	req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
	if err != nil {
		bp.logger.Warn("❌ [批量预取] 创建请求失败: ItemId=%s err=%v", itemID, err)
		return false, false, false
	}

	reqHeaders := http.Header{}
	if authHeaders != nil {
		reqHeaders = authHeaders.Clone()
	}
	reqHeaders.Del("Accept-Encoding")
	reqHeaders.Set("Accept", "application/json")
	reqHeaders.Set("X-Fnysfd-Next-Prefetch", "1")

	req.Header = reqHeaders
	req.Host = targetURL.Host

	// ✅ 用 retryClient（20s 超时）
	resp, err := bp.server.retryClient.Do(req)
	if err != nil {
		bp.logger.Debug("⚠️ [批量预取] 请求失败: ItemId=%s err=%v", itemID, err)
		return false, false, false
	}

	if resp.StatusCode != http.StatusOK {
		bodyErr, _ := io.ReadAll(io.LimitReader(resp.Body, 1*1024))
		resp.Body.Close()
		bp.logger.Warn("⚠️ [批量预取] 非200响应: ItemId=%s status=%d body=%s",
			itemID, resp.StatusCode, string(bodyErr))
		return false, false, false
	}

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
