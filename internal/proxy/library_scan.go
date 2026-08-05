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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// 辅助结构体

// libraryInfo 媒体库信息（从 /emby/Users/{uid}/Views 响应提取）
type libraryInfo struct {
	ID   string
	Name string
}

// seasonInfo 季信息（从 /emby/Shows/{seriesId}/Seasons 响应提取）
type seasonInfo struct {
	ID   string
	Name string
}

// jsonListResponse Emby 列表 API 通用响应结构
// 适用于 /Items、/Views、/Seasons、/Episodes 等列表端点
type jsonListResponse struct {
	Items            []jsonItem `json:"Items"`
	TotalRecordCount int        `json:"TotalRecordCount"`
}

// jsonItem Emby Item 通用结构（仅提取预取所需字段）
type jsonItem struct {
	Id   string `json:"Id"`
	Name string `json:"Name"`
	Type string `json:"Type"`
}

// 全库扫描安全阀：单次扫描总项目数上限
const maxScanItems = 10000

// 批量预取刷新阈值：每累积 batchFlushSize 个项目调用一次 PrefetchBatch
const batchFlushSize = 50

// 内存安全阈值：超过此值（MB）时暂停扫描并触发 GC
const memSafetyThresholdMB = 400

// LibraryScanner 全库扫描预取器
//
// 定时或手动触发扫描整个媒体库，批量预取 PlaybackInfo。
//
// 性能限制（避免容器卡死）：
//   - 信号量并发控制（batch.PrefetchBatch 内部已有）
//   - 每页之间 time.Sleep(intervalMs) 强制间隔
//   - 全局 stopCh 可中断
//   - atomic.Bool 防止并发扫描
//   - 单次扫描总项目数上限（安全阀 maxScanItems）
//   - 每批处理后内存检查，超过阈值自动 GC 暂停（memSafetyThresholdMB）
type LibraryScanner struct {
	server        *Server
	batch         *BatchPrefetcher
	logger        *logger.Logger
	authStore     *AuthStore
	stopCh        chan struct{}
	stopOnce      sync.Once // 保证 Stop() 幂等，避免重复 close 通道导致 panic
	running       atomic.Bool
	lastScanTime  time.Time
	lastScanStats BatchStats
}

// NewLibraryScanner 创建全库扫描器
func NewLibraryScanner(s *Server, b *BatchPrefetcher, auth *AuthStore) *LibraryScanner {
	return &LibraryScanner{
		server:    s,
		batch:     b,
		logger:    s.logger,
		authStore: auth,
		stopCh:    make(chan struct{}),
	}
}

// Start 启动定时扫描
//
//   - 检查 GetEnableLibraryScan 开关，未开启则直接返回
//   - 解析 cron "HH:MM" 格式，启动定时调度 goroutine
//   - 可选：启动后立即扫描（GetLibraryScanOnStart）
func (ls *LibraryScanner) Start() {
	if !config.Global.GetEnableLibraryScan() {
		ls.logger.Info("📚 [全库扫描] 功能未开启，跳过启动")
		return
	}

	// 启动定时扫描
	cron := config.Global.GetLibraryScanCron()
	if cron != "" {
		go ls.cronScheduler(cron)
		ls.logger.Info("📚 [全库扫描] 定时任务已启动: %s", cron)
	} else {
		ls.logger.Info("📚 [全库扫描] 未配置定时任务 (library_scan_cron 为空)")
	}

	// 启动后立即扫描
	if config.Global.GetLibraryScanOnStart() {
		go func() {
			ls.logger.Info("📚 [全库扫描] 启动后立即扫描")
			ls.scanOnce(context.Background())
		}()
	}
}

// cronScheduler 定时调度器
//
// 解析 cron "HH:MM" 格式，计算到下次扫描的时间间隔，循环调度。
// 收到 stopCh 信号时退出。
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

// calcNextDelay 计算 cron "HH:MM" 到下次触发的时间间隔
//
// 格式：HH:MM（24小时制），如 "03:00" 表示每天凌晨3点
// 如果今天的时间已过，则计算到明天同一时间
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
	// 如果今天的目标时间已过，定到明天
	if !next.After(now) {
		next = next.Add(24 * time.Hour)
	}

	return next.Sub(now), nil
}

// Stop 停止扫描（幂等：可安全多次调用）
func (ls *LibraryScanner) Stop() {
	ls.stopOnce.Do(func() {
		close(ls.stopCh)
	})
}

// TriggerScan 手动触发扫描（dashboard 调用）
//
// running=true 时返回错误，避免并发扫描。
// 内部通过 scanOnce 的 CAS 再次保证原子性。
func (ls *LibraryScanner) TriggerScan() error {
	if ls.running.Load() {
		return fmt.Errorf("扫描正在进行中，请稍后再试")
	}
	go ls.scanOnce(context.Background())
	return nil
}

// IsRunning 是否正在扫描
func (ls *LibraryScanner) IsRunning() bool {
	return ls.running.Load()
}

// GetStatus 获取扫描状态
//
// 返回包含 running/lastScanTime/lastScanStats 的 map
func (ls *LibraryScanner) GetStatus() map[string]interface{} {
	status := map[string]interface{}{
		"running": ls.running.Load(),
		"lastScanStats": map[string]interface{}{
			"total":   ls.lastScanStats.Total,
			"success": ls.lastScanStats.Success,
			"skipped": ls.lastScanStats.Skipped,
			"failed":  ls.lastScanStats.Failed,
		},
	}
	if !ls.lastScanTime.IsZero() {
		status["lastScanTime"] = ls.lastScanTime.Format("2006-01-02 15:04:05")
	}
	return status
}

// scanOnce 执行一次完整扫描
//
// 流程：
//  1. CAS 防止并发扫描
//  2. 从 authStore 获取认证信息，未就绪则等待（最多 10 分钟，每 30 秒检查一次）
//  3. 查询所有媒体库: GET /emby/Users/{userId}/Views
//  4. 遍历每个库，分页查询: GET /emby/Users/{userId}/Items?ParentId={libId}&Recursive=true&IncludeItemTypes=Movie,Series&StartIndex={n}&Limit=200
//  5. 对 Movie: 直接加入预取列表
//  6. 对 Series: 查询季列表，每季查询前 N 集，加入预取列表
//  7. 每页之间 time.Sleep(intervalMs)
//  8. 每累积一批（batchFlushSize 个）调用 batch.PrefetchBatch
//  9. 全部完成后更新 lastScanTime 和 lastScanStats
//
// 安全阀：单次扫描总项目数上限 maxScanItems，超过打 Warn 并停止
func (ls *LibraryScanner) scanOnce(ctx context.Context) {
	// CAS 防止并发扫描
	if !ls.running.CompareAndSwap(false, true) {
		ls.logger.Warn("📚 [全库扫描] 已有扫描在进行中，跳过")
		return
	}
	defer ls.running.Store(false)

	ls.logger.Info("📚 [全库扫描] 开始")
	startTime := time.Now()

	// 1. 等待认证信息就绪
	userID, authHeaders, ok := ls.waitForAuth(ctx)
	if !ok {
		ls.logger.Warn("📚 [全库扫描] 认证信息未就绪，等待超时，取消扫描")
		return
	}
	ls.logger.Info("📚 [全库扫描] 认证就绪，UserID=%s", userID)

	// 2. 查询所有媒体库
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

	// 3. 遍历每个库，分页查询并收集预取项
	var allStats BatchStats
	totalItems := 0
	var pending []PrefetchItem

	// flushBatch 将累积的预取项提交给 BatchPrefetcher，并累加统计
	flushBatch := func() {
		if len(pending) == 0 {
			return
		}
		stats := ls.batch.PrefetchBatch(ctx, pending, authHeaders, "library", 3600, "全库扫描")
		allStats.Total += stats.Total
		allStats.Success += stats.Success
		allStats.Skipped += stats.Skipped
		allStats.Failed += stats.Failed
		pending = pending[:0]

		// 内存安全检查：每批处理后检查内存，超过阈值则暂停并 GC
		ls.checkMemoryAndYield(ctx)
	}

	// finishScan 统一处理扫描结束：刷新剩余批次，更新状态，输出日志
	finishScan := func(reason string) {
		flushBatch()
		ls.lastScanTime = time.Now()
		ls.lastScanStats = allStats
		if reason != "" {
			ls.logger.Info("📚 [全库扫描] 结束(%s): 总计=%d 成功=%d 跳过=%d 失败=%d 耗时=%v",
				reason, allStats.Total, allStats.Success, allStats.Skipped, allStats.Failed, time.Since(startTime))
		} else {
			ls.logger.Info("📚 [全库扫描] 完成: 总计=%d 成功=%d 跳过=%d 失败=%d 耗时=%v",
				allStats.Total, allStats.Success, allStats.Skipped, allStats.Failed, time.Since(startTime))
		}
	}

	for _, lib := range libraries {
		// 检查停止信号
		select {
		case <-ls.stopCh:
			ls.logger.Info("📚 [全库扫描] 收到停止信号，中止扫描")
			finishScan("已停止")
			return
		default:
		}

		ls.logger.Info("📚 [全库扫描] 扫描媒体库: %s (ID=%s)", lib.Name, lib.ID)

		// 分页查询
		startIndex := 0
		const pageSize = 200
		for {
			// 检查停止信号
			select {
			case <-ls.stopCh:
				ls.logger.Info("📚 [全库扫描] 收到停止信号，中止扫描")
				finishScan("已停止")
				return
			default:
			}

			items, total, err := ls.queryItems(ctx, userID, authHeaders, lib.ID, startIndex, pageSize)
			if err != nil {
				ls.logger.Warn("📚 [全库扫描] 查询项目失败: 库=%s startIndex=%d err=%v", lib.Name, startIndex, err)
				break
			}

			for _, item := range items {
				totalItems++
				// 安全阀：总项目数上限
				if totalItems > maxScanItems {
					ls.logger.Warn("📚 [全库扫描] 达到安全阀上限 %d，停止扫描", maxScanItems)
					finishScan("安全阀")
					return
				}
				pending = append(pending, item)
				// 每累积一批调用 PrefetchBatch
				if len(pending) >= batchFlushSize {
					flushBatch()
				}
			}

			// 判断是否还有下一页
			startIndex += pageSize
			if startIndex >= total || len(items) == 0 {
				break
			}

			// 每页之间强制间隔
			if !ls.sleep(ctx) {
				finishScan("已停止")
				return
			}
		}
	}

	// 4. 全部完成
	finishScan("")
}

// waitForAuth 等待 authStore 认证信息就绪
//
// 未就绪则等待（最多 10 分钟，每 30 秒检查一次，打 Warn 日志）。
// 收到 stopCh 或 ctx.Done() 时提前退出。
func (ls *LibraryScanner) waitForAuth(ctx context.Context) (string, http.Header, bool) {
	const maxWait = 10 * time.Minute
	const checkInterval = 30 * time.Second

	deadline := time.Now().Add(maxWait)

	for {
		// 检查 authStore 是否就绪
		if ls.authStore.IsReady() {
			// Get 返回 (headers, userID, expired)，IsReady 已确认未过期
			headers, userID, expired := ls.authStore.Get()
			if !expired && userID != "" && headers != nil {
				return userID, headers, true
			}
		}

		// 检查超时
		if time.Now().After(deadline) {
			return "", nil, false
		}

		ls.logger.Warn("📚 [全库扫描] 认证信息未就绪，等待 %v 后重试...", checkInterval)

		select {
		case <-time.After(checkInterval):
		case <-ls.stopCh:
			return "", nil, false
		case <-ctx.Done():
			return "", nil, false
		}
	}
}

// sleep 按配置的间隔休眠
//
// 返回 false 表示收到停止信号或上下文取消，调用方应中止扫描。
func (ls *LibraryScanner) sleep(ctx context.Context) bool {
	intervalMs := config.Global.GetLibraryScanIntervalMs()
	if intervalMs <= 0 {
		return true
	}
	select {
	case <-time.After(time.Duration(intervalMs) * time.Millisecond):
		return true
	case <-ls.stopCh:
		return false
	case <-ctx.Done():
		return false
	}
}

// checkMemoryAndYield 检查运行时内存，超过阈值时暂停扫描并触发 GC
//
// 防止全库扫描期间内存持续增长导致容器 OOM 卡死。
// 超过 memSafetyThresholdMB 时：
//  1. 打 Warn 日志
//  2. 调用 runtime.GC() 释放未引用的内存
//  3. 额外等待 2 秒让 GC 完成（期间响应停止信号）
func (ls *LibraryScanner) checkMemoryAndYield(ctx context.Context) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	memMB := float64(m.Alloc) / 1024 / 1024

	if memMB > memSafetyThresholdMB {
		ls.logger.Warn("📚 [全库扫描] 内存过高 (%.1fMB/%dMB)，暂停扫描并触发GC", memMB, memSafetyThresholdMB)
		runtime.GC()

		var m2 runtime.MemStats
		runtime.ReadMemStats(&m2)
		newMemMB := float64(m2.Alloc) / 1024 / 1024
		ls.logger.Info("📚 [全库扫描] GC完成，内存释放: %.1fMB -> %.1fMB", memMB, newMemMB)

		// 额外等待 2 秒让系统稳定，期间响应停止信号
		select {
		case <-time.After(2 * time.Second):
		case <-ls.stopCh:
		case <-ctx.Done():
		}
	}
}

// doRequest 构造并发送 HTTP 请求
//
// 请求构造复用项目模式：
//   - 克隆 authHeaders（认证头来自 authStore 捕获的客户端请求）
//   - 删 Accept-Encoding（让 Transport 自动处理 gzip）
//   - 设 Accept: application/json（强制 JSON 响应，避免飞牛返回 HTML）
//   - 用 server.retryClient 发送
func (ls *LibraryScanner) doRequest(ctx context.Context, authHeaders http.Header, path string) (*http.Response, error) {
	// 通过 proxyMu 保护读取 targetURL（与 Reload/handleProxy 并发安全）
	ls.server.proxyMu.RLock()
	targetURL := ls.server.targetURL
	ls.server.proxyMu.RUnlock()

	fullURL := targetURL.Scheme + "://" + targetURL.Host + path

	req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
	if err != nil {
		return nil, err
	}

	// 克隆认证头
	if authHeaders != nil {
		req.Header = authHeaders.Clone()
	}
	// 删 Accept-Encoding，让 Transport 自动处理 gzip
	req.Header.Del("Accept-Encoding")
	// 强制 JSON 响应，避免飞牛返回 HTML
	req.Header.Set("Accept", "application/json")
	req.Host = targetURL.Host

	return ls.server.retryClient.Do(req)
}

// queryViews 查询所有媒体库
//
// GET /emby/Users/{userId}/Views
// 返回媒体库列表（ID + Name）
func (ls *LibraryScanner) queryViews(ctx context.Context, userID string, authHeaders http.Header) ([]libraryInfo, error) {
	path := "/emby/Users/" + userID + "/Views"

	resp, err := ls.doRequest(ctx, authHeaders, path)
	if err != nil {
		return nil, fmt.Errorf("请求失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("查询媒体库失败: status=%d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 5*1024*1024))
	if err != nil {
		return nil, fmt.Errorf("读取响应体失败: %w", err)
	}

	var listResp jsonListResponse
	if err := json.Unmarshal(body, &listResp); err != nil {
		return nil, fmt.Errorf("JSON解析失败: %w", err)
	}

	libraries := make([]libraryInfo, 0, len(listResp.Items))
	for _, item := range listResp.Items {
		if item.Id == "" {
			continue
		}
		libraries = append(libraries, libraryInfo{
			ID:   item.Id,
			Name: item.Name,
		})
	}

	return libraries, nil
}

// queryItems 分页查询指定媒体库的项目
//
// GET /emby/Users/{userId}/Items?ParentId={libId}&Recursive=true&IncludeItemTypes=Movie,Series&StartIndex={n}&Limit=200
//
// 对 Movie: 直接加入预取列表
// 对 Series: 查询季列表，每季查询前 N 集（GetLibraryScanEpisodeCount），加入预取列表
//
// 返回 ([]PrefetchItem, totalRecordCount, error)
// totalRecordCount 用于分页判断是否还有下一页
func (ls *LibraryScanner) queryItems(ctx context.Context, userID string, authHeaders http.Header, parentID string, startIndex, limit int) ([]PrefetchItem, int, error) {
	// 构造查询参数
	query := url.Values{}
	query.Set("ParentId", parentID)
	query.Set("Recursive", "true")
	query.Set("IncludeItemTypes", "Movie,Series")
	query.Set("StartIndex", strconv.Itoa(startIndex))
	query.Set("Limit", strconv.Itoa(limit))

	path := "/emby/Users/" + userID + "/Items?" + query.Encode()

	resp, err := ls.doRequest(ctx, authHeaders, path)
	if err != nil {
		return nil, 0, fmt.Errorf("请求失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, 0, fmt.Errorf("查询项目失败: status=%d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 5*1024*1024))
	if err != nil {
		return nil, 0, fmt.Errorf("读取响应体失败: %w", err)
	}

	var listResp jsonListResponse
	if err := json.Unmarshal(body, &listResp); err != nil {
		return nil, 0, fmt.Errorf("JSON解析失败: %w", err)
	}

	var result []PrefetchItem

	for _, item := range listResp.Items {
		switch item.Type {
		case "Movie":
			// Movie 直接加入预取列表
			if item.Id != "" {
				result = append(result, PrefetchItem{
					ItemID: item.Id,
					UserID: userID,
					Name:   item.Name,
					Type:   item.Type,
				})
			}

		case "Series":
			// Series 查询季列表，每季查询前 N 集
			seasons, err := ls.querySeasons(ctx, userID, authHeaders, item.Id)
			if err != nil {
				ls.logger.Debug("📚 [全库扫描] 查询季列表失败: 剧集=%s err=%v", item.Name, err)
				continue
			}

			for _, season := range seasons {
				// 检查停止信号
				select {
				case <-ls.stopCh:
					return result, listResp.TotalRecordCount, nil
				default:
				}

				// 查询前 N 集
				episodes, err := ls.queryEpisodes(ctx, userID, authHeaders, item.Id, season.ID)
				if err != nil {
					ls.logger.Debug("📚 [全库扫描] 查询集列表失败: 剧集=%s 季=%s err=%v",
						item.Name, season.Name, err)
					continue
				}
				result = append(result, episodes...)

				// 每季查询后强制间隔，避免请求过密
				if !ls.sleep(ctx) {
					return result, listResp.TotalRecordCount, nil
				}
			}
		}
	}

	return result, listResp.TotalRecordCount, nil
}

// querySeasons 查询剧集的季列表
//
// GET /emby/Shows/{seriesId}/Seasons
// 返回季列表（ID + Name）
func (ls *LibraryScanner) querySeasons(ctx context.Context, userID string, authHeaders http.Header, seriesID string) ([]seasonInfo, error) {
	query := url.Values{}
	if userID != "" {
		query.Set("UserId", userID)
	}

	path := "/emby/Shows/" + seriesID + "/Seasons?" + query.Encode()

	resp, err := ls.doRequest(ctx, authHeaders, path)
	if err != nil {
		return nil, fmt.Errorf("请求失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("查询季列表失败: status=%d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 5*1024*1024))
	if err != nil {
		return nil, fmt.Errorf("读取响应体失败: %w", err)
	}

	var listResp jsonListResponse
	if err := json.Unmarshal(body, &listResp); err != nil {
		return nil, fmt.Errorf("JSON解析失败: %w", err)
	}

	seasons := make([]seasonInfo, 0, len(listResp.Items))
	for _, item := range listResp.Items {
		if item.Id == "" {
			continue
		}
		seasons = append(seasons, seasonInfo{
			ID:   item.Id,
			Name: item.Name,
		})
	}

	return seasons, nil
}

// queryEpisodes 查询指定季的集列表
//
// GET /emby/Shows/{seriesId}/Episodes?SeasonId={seasonId}
// 限制返回前 N 集（GetLibraryScanEpisodeCount），按集号升序排列
// 返回预取项列表（ItemID + Name）
func (ls *LibraryScanner) queryEpisodes(ctx context.Context, userID string, authHeaders http.Header, seriesID, seasonID string) ([]PrefetchItem, error) {
	episodeCount := config.Global.GetLibraryScanEpisodeCount()

	query := url.Values{}
	query.Set("SeasonId", seasonID)
	query.Set("Limit", strconv.Itoa(episodeCount))
	query.Set("SortBy", "IndexNumber")
	query.Set("SortOrder", "Ascending")
	if userID != "" {
		query.Set("UserId", userID)
	}

	path := "/emby/Shows/" + seriesID + "/Episodes?" + query.Encode()

	resp, err := ls.doRequest(ctx, authHeaders, path)
	if err != nil {
		return nil, fmt.Errorf("请求失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("查询集列表失败: status=%d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 5*1024*1024))
	if err != nil {
		return nil, fmt.Errorf("读取响应体失败: %w", err)
	}

	var listResp jsonListResponse
	if err := json.Unmarshal(body, &listResp); err != nil {
		return nil, fmt.Errorf("JSON解析失败: %w", err)
	}

	items := make([]PrefetchItem, 0, len(listResp.Items))
	for _, item := range listResp.Items {
		if item.Id == "" {
			continue
		}
		items = append(items, PrefetchItem{
			ItemID: item.Id,
			UserID: userID,
			Name:   item.Name,
			Type:   item.Type,
		})
	}

	return items, nil
}
