package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"fnysfd/internal/config"
	"fnysfd/internal/logger"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ========== 辅助结构体 ==========

type libraryInfo struct {
	ID   string
	Name string
}

type seasonInfo struct {
	ID   string
	Name string
}

// mediaStreamItem 单个流（视频/音频/字幕）
type mediaStreamItem struct {
	Type   string `json:"Type"`
	Codec  string `json:"Codec"`
	Width  int    `json:"Width"`
	Height int    `json:"Height"`
}

// mediaSourceItem 媒体源
type mediaSourceItem struct {
	ID           string            `json:"Id"`
	Path         string            `json:"Path"`
	MediaStreams []mediaStreamItem `json:"MediaStreams"`
}

type jsonListResponse struct {
	Items            []jsonItem `json:"Items"`
	TotalRecordCount int        `json:"TotalRecordCount"`
}

type jsonItem struct {
	Id             string            `json:"Id"`
	Name           string            `json:"Name"`
	Type           string            `json:"Type"`
	DateCreated    string            `json:"DateCreated"`
	RunTimeTicks   int64             `json:"RunTimeTicks"`
	ProductionYear int               `json:"ProductionYear"` // ✅ 新增
	ProviderIds    map[string]string `json:"ProviderIds"`    // ✅ 新增
	SeriesId       string            `json:"SeriesId"`       // ✅ 新增
	SeriesName     string            `json:"SeriesName"`     // ✅ 新增
	IndexNumber    int               `json:"IndexNumber"`    // ✅ 新增
	MediaSources   []mediaSourceItem `json:"MediaSources"`
}

// alreadyProbed 判断飞牛是否已真正 probe
func (item jsonItem) alreadyProbed() bool {
	for _, ms := range item.MediaSources {
		for _, s := range ms.MediaStreams {
			if s.Codec != "" {
				return true
			}
		}
	}
	return false
}

// 常量
const maxScanItems = 10000
const batchFlushSize = 3
const memSafetyThresholdMB = 400
const incrementalLimit = 200

// LibraryScanner 全库扫描预取器
type LibraryScanner struct {
	server        *Server
	batch         *BatchPrefetcher
	logger        *logger.Logger
	authStore     *AuthStore
	stopCh        chan struct{}
	stopOnce      sync.Once
	wg            sync.WaitGroup
	running       atomic.Bool
	lastScanTime  time.Time
	lastScanStats BatchStats

	lastIncScanTime  time.Time
	lastIncScanStats BatchStats

	lastGCTime atomic.Int64
}

func NewLibraryScanner(s *Server, b *BatchPrefetcher, auth *AuthStore) *LibraryScanner {
	return &LibraryScanner{
		server:    s,
		batch:     b,
		logger:    s.logger,
		authStore: auth,
		stopCh:    make(chan struct{}),
	}
}

// ============================================================
// 启动 / 停止
// ============================================================

func (ls *LibraryScanner) Start() {
	if !config.Global.GetEnableLibraryScan() {
		ls.logger.Info("📚 [全库扫描] 功能未开启，跳过启动")
		return
	}

	cron := config.Global.GetLibraryScanCron()
	if cron != "" {
		ls.wg.Add(1)
		go func() {
			defer ls.wg.Done()
			ls.cronScheduler(cron)
		}()
		ls.logger.Info("📚 [全库扫描] 定时任务已启动: 每日 %s", cron)
	} else {
		ls.logger.Info("📚 [全库扫描] 未配置定时任务 (library_scan_cron 为空)")
	}

	if config.Global.GetLibraryScanOnStart() {
		ls.wg.Add(1)
		go func() {
			defer ls.wg.Done()
			ls.logger.Info("📚 [全库扫描] 启动后立即扫描")
			ls.scanOnce(context.Background())
		}()
	}

	intervalMinutes := config.Global.GetLibraryScanIncrementalMinutes()
	interval := time.Duration(intervalMinutes) * time.Minute

	ls.wg.Add(1)
	go func() {
		defer ls.wg.Done()
		ls.incrementalScheduler(interval)
	}()
	ls.logger.Info("📚 [增量扫描] 定时任务已启动: 每 %v 一次 (Limit=%d)", interval, incrementalLimit)
}

func (ls *LibraryScanner) Stop() {
	ls.stopOnce.Do(func() {
		close(ls.stopCh)
	})
	ls.wg.Wait()
}

// ============================================================
// 全库扫描调度
// ============================================================

func (ls *LibraryScanner) cronScheduler(cron string) {
	for {
		nextDelay, err := ls.calcNextDelay(cron)
		if err != nil {
			ls.logger.Warn("📚 [全库扫描] cron 解析失败，停止定时调度: %v", err)
			return
		}
		ls.logger.Info("📚 [全库扫描] 下次扫描: %v 后", nextDelay)
		select {
		case <-time.After(nextDelay):
			ls.scanOnce(context.Background())
		case <-ls.stopCh:
			return
		}
	}
}

func (ls *LibraryScanner) calcNextDelay(cron string) (time.Duration, error) {
	parts := strings.Split(cron, ":")
	if len(parts) != 2 {
		return 0, fmt.Errorf("cron 格式错误，应为 HH:MM: %s", cron)
	}
	hour, err := strconv.Atoi(parts[0])
	if err != nil || hour < 0 || hour > 23 {
		return 0, fmt.Errorf("cron 小时无效: %s", parts[0])
	}
	minute, err := strconv.Atoi(parts[1])
	if err != nil || minute < 0 || minute > 59 {
		return 0, fmt.Errorf("cron 分钟无效: %s", parts[1])
	}
	now := time.Now()
	next := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, 0, 0, time.Local)
	if !next.After(now) {
		next = next.Add(24 * time.Hour)
	}
	return next.Sub(now), nil
}

// ============================================================
// 增量扫描调度
// ============================================================

func (ls *LibraryScanner) incrementalScheduler(interval time.Duration) {
	select {
	case <-time.After(30 * time.Second):
	case <-ls.stopCh:
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ls.scanIncremental(context.Background())
		case <-ls.stopCh:
			return
		}
	}
}

func (ls *LibraryScanner) scanIncremental(ctx context.Context) {
	if !ls.running.CompareAndSwap(false, true) {
		ls.logger.Debug("📚 [增量扫描] 已有扫描在进行中，跳过本次")
		return
	}
	defer ls.running.Store(false)

	startTime := time.Now()
	ls.logger.Info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	ls.logger.Info("📚 [增量扫描] 开始")
	ls.logger.Info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	userID, authHeaders, ok := ls.waitForAuth(ctx)
	if !ok {
		ls.logger.Warn("📚 [增量扫描] 认证未就绪，跳过")
		return
	}

	libraries, err := ls.queryViews(ctx, userID, authHeaders)
	if err != nil {
		ls.logger.Warn("📚 [增量扫描] 查询媒体库失败: %v", err)
		return
	}
	if len(libraries) == 0 {
		ls.logger.Info("📚 [增量扫描] 无媒体库，结束")
		return
	}

	ls.logger.Info("📚 [增量扫描] 发现 %d 个媒体库，逐个拉最新 %d 项", len(libraries), incrementalLimit)

	var allStats BatchStats
	totalFetched := 0
	totalSkippedCached := 0
	totalSkippedProbed := 0
	totalCandidates := 0
	libCount := 0

	for _, lib := range libraries {
		select {
		case <-ls.stopCh:
			ls.logger.Info("📚 [增量扫描] 收到停止信号，中止")
			return
		case <-ctx.Done():
			return
		default:
		}

		ls.logger.Info("────────────────────────────────────────")
		ls.logger.Info("📂 [增量扫描] 处理媒体库: %s (ID=%s)", lib.Name, lib.ID)

		items, err := ls.queryLatestItems(ctx, userID, authHeaders, lib.ID, incrementalLimit)
		if err != nil {
			ls.logger.Warn("📚 [增量扫描] 库 %s 拉最新项失败: %v", lib.Name, err)
			continue
		}
		libCount++
		totalFetched += len(items)

		if len(items) == 0 {
			ls.logger.Info("📂 [增量扫描] 库 %s: 无最新项", lib.Name)
			continue
		}

		ls.logger.Info("📂 [增量扫描] 库 %s: 拉到 %d 项，开始逐项判断", lib.Name, len(items))

		var candidates []PrefetchItem
		skippedCached := 0
		skippedProbed := 0

		for i, item := range items {
			// ✅ 触发豆瓣抓取（异步）
			if ls.server.doubanProvider != nil && doubanEnabled {
				switch item.Type {
				case "Movie", "Series":
					go ls.server.doubanProvider.FetchMovieOrSeries(ctx, item.ImdbID, item.Name, item.ProductionYear, item.Type)
				}
			}

			cacheHit := false
			if source, found := ls.server.cache.GetByItemID(item.ItemID); found {
				if _, urlFound := ls.server.cache.GetStreamURL(source.ID); urlFound {
					cacheHit = true
				}
			}

			switch {
			case cacheHit:
				skippedCached++
				ls.logger.Info("   [%2d/%d] %s (%s) | 缓存=命中 | 判断=跳过(已缓存)",
					i+1, len(items), item.Name, item.Type)
			case item.AlreadyProbed:
				skippedProbed++
				ls.logger.Info("   [%2d/%d] %s (%s) | 缓存=miss | probe=已probe | 判断=跳过(飞牛已探测)",
					i+1, len(items), item.Name, item.Type)
			default:
				candidates = append(candidates, item)
				ls.logger.Info("   [%2d/%d] %s (%s) | 缓存=miss | probe=未probe | 判断=加入预取 ✅",
					i+1, len(items), item.Name, item.Type)
			}
		}

		totalSkippedCached += skippedCached
		totalSkippedProbed += skippedProbed

		ls.logger.Info("📂 [增量扫描] 库 %s 判断完成: 待预取=%d 缓存命中=%d 已probe跳过=%d",
			lib.Name, len(candidates), skippedCached, skippedProbed)

		if len(candidates) == 0 {
			continue
		}

		totalCandidates += len(candidates)

		ls.logger.Info("🚀 [增量扫描] 库 %s 开始预取 %d 项...", lib.Name, len(candidates))
		var wg sync.WaitGroup
		var mu sync.Mutex
		for _, c := range candidates {
			c := c
			wg.Add(1)
			go func() {
				defer wg.Done()

				hit, deduped, prefetched := ls.batch.PrefetchItem(
					ctx, c.ItemID, c.UserID, authHeaders, "library_inc", 3600,
				)

				mu.Lock()
				allStats.Total++
				switch {
				case hit:
					allStats.Skipped++
					ls.logger.Info("   ⏭️ 预取结果: %s (%s) → 缓存命中", c.Name, c.Type)
				case deduped:
					allStats.Deduped++
					ls.logger.Info("   ⏭️ 预取结果: %s (%s) → 去重跳过", c.Name, c.Type)
				case prefetched:
					allStats.Success++
					ls.logger.Info("   ✅ 预取结果: %s (%s) → 成功", c.Name, c.Type)
				default:
					allStats.Failed++
					ls.logger.Warn("   ⚠️ 预取结果: %s (%s) → 失败", c.Name, c.Type)
				}
				mu.Unlock()
			}()
		}
		wg.Wait()

		select {
		case <-time.After(300 * time.Millisecond):
		case <-ls.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}

	elapsed := time.Since(startTime)
	ls.lastIncScanTime = time.Now()
	ls.lastIncScanStats = allStats

	ls.logger.Info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	ls.logger.Info("📚 [增量扫描] 完成")
	ls.logger.Info("   库数: %d | 拉到: %d 项", libCount, totalFetched)
	ls.logger.Info("   判断: 缓存命中跳过=%d, 已probe跳过=%d, 待预取=%d",
		totalSkippedCached, totalSkippedProbed, totalCandidates)
	ls.logger.Info("   预取: 成功=%d, 跳过=%d, 去重=%d, 失败=%d",
		allStats.Success, allStats.Skipped, allStats.Deduped, allStats.Failed)
	ls.logger.Info("   耗时: %v", elapsed)
	ls.logger.Info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
}

// queryLatestItems 拉某个库的最新 N 项
func (ls *LibraryScanner) queryLatestItems(ctx context.Context, userID string, authHeaders http.Header, parentID string, limit int) ([]PrefetchItem, error) {
	if limit <= 0 {
		limit = incrementalLimit
	}

	query := url.Values{}
	query.Set("ParentId", parentID)
	query.Set("Limit", strconv.Itoa(limit))
	query.Set("Fields", "Path,MediaSources,MediaStreams,ProviderIds,ProductionYear") // ✅ 加

	path := "/emby/Users/" + userID + "/Items/Latest?" + query.Encode()

	resp, err := ls.doRequest(ctx, authHeaders, path)
	if err != nil {
		return nil, fmt.Errorf("请求失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("查询最新项失败: status=%d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 20*1024*1024))
	if err != nil {
		return nil, fmt.Errorf("读取响应体失败: %w", err)
	}

	var items []jsonItem
	if err := json.Unmarshal(body, &items); err != nil {
		return nil, fmt.Errorf("JSON解析失败: %w", err)
	}

	return ls.expandItems(ctx, userID, authHeaders, items), nil
}

// expandItems 把 Items 展开为 PrefetchItem 列表
func (ls *LibraryScanner) expandItems(ctx context.Context, userID string, authHeaders http.Header, items []jsonItem) []PrefetchItem {
	var result []PrefetchItem
	for _, item := range items {
		if item.Id == "" {
			continue
		}

		imdbID := ""
		if item.ProviderIds != nil {
			imdbID = item.ProviderIds["Imdb"]
		}

		switch item.Type {
		case "Movie", "Video", "Episode":
			result = append(result, PrefetchItem{
				ItemID:         item.Id,
				UserID:         userID,
				Name:           item.Name,
				Type:           item.Type,
				AlreadyProbed:  item.alreadyProbed(),
				ImdbID:         imdbID,
				ProductionYear: item.ProductionYear,
				SeriesID:       item.SeriesId,
				SeriesName:     item.SeriesName,
				SeasonNumber:   item.IndexNumber,
			})
		case "Series":
			// ✅ 触发整剧评分抓取（用 IMDb）
			if ls.server.doubanProvider != nil && doubanEnabled {
				go ls.server.doubanProvider.FetchMovieOrSeries(
					ctx, imdbID, item.Name, item.ProductionYear, "Series",
				)
			}

			select {
			case <-ls.stopCh:
				return result
			case <-ctx.Done():
				return result
			default:
			}
			episodes, err := ls.querySeriesEpisodes(ctx, userID, authHeaders, item.Id, item.Name)
			if err != nil {
				ls.logger.Debug("📚 [扫描] 展开剧集失败: %s err=%v", item.Name, err)
				continue
			}
			result = append(result, episodes...)
		default:
			ls.logger.Debug("📚 [扫描] 跳过未识别类型: %s (%s)", item.Name, item.Type)
		}
	}
	return result
}

// ============================================================
// 状态查询
// ============================================================

func (ls *LibraryScanner) TriggerScan() error {
	if !ls.running.CompareAndSwap(false, true) {
		return fmt.Errorf("扫描正在进行中，请稍后再试")
	}
	ls.wg.Add(1)
	go func() {
		defer ls.wg.Done()
		defer ls.running.Store(false)
		ls.scanOnceLocked(context.Background())
	}()
	return nil
}

func (ls *LibraryScanner) IsRunning() bool {
	return ls.running.Load()
}

func (ls *LibraryScanner) GetStatus() map[string]interface{} {
	intervalMinutes := config.Global.GetLibraryScanIncrementalMinutes()

	status := map[string]interface{}{
		"running":             ls.running.Load(),
		"incrementalInterval": (time.Duration(intervalMinutes) * time.Minute).String(),
		"incrementalMinutes":  intervalMinutes,
		"incrementalLimit":    incrementalLimit,
		"lastScanStats": map[string]interface{}{
			"total":     ls.lastScanStats.Total,
			"success":   ls.lastScanStats.Success,
			"skipped":   ls.lastScanStats.Skipped,
			"deduped":   ls.lastScanStats.Deduped,
			"failed":    ls.lastScanStats.Failed,
			"cancelled": ls.lastScanStats.Cancelled,
		},
		"lastIncScanStats": map[string]interface{}{
			"total":     ls.lastIncScanStats.Total,
			"success":   ls.lastIncScanStats.Success,
			"skipped":   ls.lastIncScanStats.Skipped,
			"deduped":   ls.lastIncScanStats.Deduped,
			"failed":    ls.lastIncScanStats.Failed,
			"cancelled": ls.lastIncScanStats.Cancelled,
		},
	}
	if !ls.lastScanTime.IsZero() {
		status["lastScanTime"] = ls.lastScanTime.Format("2006-01-02 15:04:05")
	}
	if !ls.lastIncScanTime.IsZero() {
		status["lastIncScanTime"] = ls.lastIncScanTime.Format("2006-01-02 15:04:05")
	}
	return status
}

// ============================================================
// 全库扫描主体
// ============================================================

func (ls *LibraryScanner) scanOnce(ctx context.Context) {
	if !ls.running.CompareAndSwap(false, true) {
		ls.logger.Warn("📚 [全库扫描] 已有扫描在进行中，跳过")
		return
	}
	defer ls.running.Store(false)
	ls.scanOnceLocked(ctx)
}

func (ls *LibraryScanner) scanOnceLocked(ctx context.Context) {
	startTime := time.Now()
	ls.logger.Info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	ls.logger.Info("📚 [全库扫描] 开始")
	ls.logger.Info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	userID, authHeaders, ok := ls.waitForAuth(ctx)
	if !ok {
		ls.logger.Warn("📚 [全库扫描] 认证信息未就绪，等待超时，取消扫描")
		return
	}
	ls.logger.Info("📚 [全库扫描] 认证就绪，UserID=%s", userID)

	libraries, err := ls.queryViews(ctx, userID, authHeaders)
	if err != nil {
		ls.logger.Warn("📚 [全库扫描] 查询媒体库失败: %v", err)
		return
	}
	if len(libraries) == 0 {
		ls.logger.Warn("📚 [全库扫描] 未发现任何媒体库")
		return
	}
	ls.logger.Info("📚 [全库扫描] 发现 %d 个媒体库", len(libraries))

	var allStats BatchStats
	totalItems := 0
	totalSkippedCached := 0
	totalSkippedProbed := 0
	var pending []PrefetchItem
	var failedItems []PrefetchItem

	flushBatch := func() {
		if len(pending) == 0 {
			return
		}
		stats := ls.batch.PrefetchBatch(ctx, pending, authHeaders, "library", 3600, "全库扫描")
		allStats.Total += stats.Total
		allStats.Success += stats.Success
		allStats.Skipped += stats.Skipped
		allStats.Deduped += stats.Deduped
		allStats.Failed += stats.Failed
		allStats.Cancelled += stats.Cancelled

		if stats.Failed > 0 {
			for _, item := range pending {
				failedItems = append(failedItems, item)
			}
		}

		pending = pending[:0]
		ls.checkMemoryAndYield(ctx)

		select {
		case <-time.After(800 * time.Millisecond):
		case <-ls.stopCh:
		case <-ctx.Done():
		}
	}

	retryFailed := func() {
		if len(failedItems) == 0 {
			return
		}
		ls.logger.Info("📚 [全库扫描] 重试 %d 个失败项", len(failedItems))

		for i := 0; i < len(failedItems); i += 2 {
			select {
			case <-ls.stopCh:
				return
			case <-ctx.Done():
				return
			default:
			}

			end := i + 2
			if end > len(failedItems) {
				end = len(failedItems)
			}
			batch := failedItems[i:end]

			stats := ls.batch.PrefetchBatch(ctx, batch, authHeaders, "library_retry", 60, "全库扫描-重试")
			allStats.Total += stats.Total
			allStats.Success += stats.Success
			allStats.Skipped += stats.Skipped
			allStats.Deduped += stats.Deduped
			allStats.Failed += stats.Failed
			allStats.Cancelled += stats.Cancelled

			select {
			case <-time.After(1 * time.Second):
			case <-ls.stopCh:
			case <-ctx.Done():
				return
			}
		}
		ls.logger.Info("📚 [全库扫描] 重试完成")
	}

	finishScan := func(reason string) {
		flushBatch()
		retryFailed()
		ls.lastScanTime = time.Now()
		ls.lastScanStats = allStats

		ls.logger.Info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		if reason != "" {
			ls.logger.Info("📚 [全库扫描] 结束 (%s)", reason)
		} else {
			ls.logger.Info("📚 [全库扫描] 完成")
		}
		ls.logger.Info("   遍历项数: %d", totalItems)
		ls.logger.Info("   判断: 缓存命中跳过=%d, 已probe跳过=%d",
			totalSkippedCached, totalSkippedProbed)
		ls.logger.Info("   预取: 总计=%d 成功=%d 跳过=%d 去重=%d 失败=%d 取消=%d",
			allStats.Total, allStats.Success, allStats.Skipped,
			allStats.Deduped, allStats.Failed, allStats.Cancelled)
		ls.logger.Info("   耗时: %v", time.Since(startTime))
		ls.logger.Info("━━━━━━━━━━━━━━━━━━━━━━
