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

// ========== FNOS API 专用结构体定义 ==========

// fnosLibraryListResponse FNOS /v/api/v1/mediadb/list 接口响应
type fnosLibraryListResponse struct {
	Msg  string            `json:"msg"`
	Code int               `json:"code"` // 0 表示成功
	Data []fnosLibraryItem `json:"data"`
}

// fnosLibraryItem FNOS 媒体库项
type fnosLibraryItem struct {
	GUID     string `json:"guid"`     // 媒体库 ID
	Title    string `json:"title"`    // 媒体库名称
	Category string `json:"category"` // Movie / Series
}

// fnosPlayListResponse FNOS /v/api/v1/play/list 接口响应
type fnosPlayListResponse struct {
	Msg  string         `json:"msg"`
	Code int            `json:"code"` // 0 表示成功
	Data []fnosPlayItem `json:"data"`
}

// fnosPlayItem FNOS 影片项
type fnosPlayItem struct {
	ID   string `json:"id"`   // 影片 ID
	Name string `json:"name"` // 影片名称
	Type string `json:"type"` // Movie / Series
}

// fnosSeasonListResponse FNOS 季列表接口响应（待完善）
type fnosSeasonListResponse struct {
	Msg  string           `json:"msg"`
	Code int              `json:"code"`
	Data []fnosSeasonItem `json:"data"`
}

type fnosSeasonItem struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// fnosEpisodeListResponse FNOS 集列表接口响应（待完善）
type fnosEpisodeListResponse struct {
	Msg  string            `json:"msg"`
	Code int               `json:"code"`
	Data []fnosEpisodeItem `json:"data"`
}

type fnosEpisodeItem struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// ========== 内部使用的基础类型 ==========

// libraryInfo 媒体库信息
type libraryInfo struct {
	ID   string
	Name string
}

// seasonInfo 季信息
type seasonInfo struct {
	ID   string
	Name string
}

// jsonListResponse 保留 Emby 风格响应（备用）
type jsonListResponse struct {
	Items            []jsonItem `json:"Items"`
	TotalRecordCount int        `json:"TotalRecordCount"`
}

type jsonItem struct {
	Id   string `json:"Id"`
	Name string `json:"Name"`
	Type string `json:"Type"`
}

// 常量
const maxScanItems = 10000
const batchFlushSize = 50
const memSafetyThresholdMB = 400

// LibraryScanner 全库扫描预取器
type LibraryScanner struct {
	server        *Server
	batch         *BatchPrefetcher
	logger        *logger.Logger
	authStore     *AuthStore
	stopCh        chan struct{}
	stopOnce      sync.Once
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
func (ls *LibraryScanner) Start() {
	if !config.Global.GetEnableLibraryScan() {
		ls.logger.Info("📚 [全库扫描] 功能未开启，跳过启动")
		return
	}
	cron := config.Global.GetLibraryScanCron()
	if cron != "" {
		go ls.cronScheduler(cron)
		ls.logger.Info("📚 [全库扫描] 定时任务已启动: %s", cron)
	} else {
		ls.logger.Info("📚 [全库扫描] 未配置定时任务 (library_scan_cron 为空)")
	}
	if config.Global.GetLibraryScanOnStart() {
		go func() {
			ls.logger.Info("📚 [全库扫描] 启动后立即扫描")
			ls.scanOnce(context.Background())
		}()
	}
}

// cronScheduler 定时调度器
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

// calcNextDelay 计算 cron 到下次触发的时间间隔
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

// Stop 停止扫描
func (ls *LibraryScanner) Stop() {
	ls.stopOnce.Do(func() {
		close(ls.stopCh)
	})
}

// TriggerScan 手动触发扫描
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
func (ls *LibraryScanner) scanOnce(ctx context.Context) {
	if !ls.running.CompareAndSwap(false, true) {
		ls.logger.Warn("📚 [全库扫描] 已有扫描在进行中，跳过")
		return
	}
	defer ls.running.Store(false)

	ls.logger.Info("📚 [全库扫描] 开始")
	startTime := time.Now()

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
	var pending []PrefetchItem

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
		ls.checkMemoryAndYield(ctx)
	}

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
		select {
		case <-ls.stopCh:
			ls.logger.Info("📚 [全库扫描] 收到停止信号，中止扫描")
			finishScan("已停止")
			return
		default:
		}

		ls.logger.Info("📚 [全库扫描] 扫描媒体库: %s (ID=%s)", lib.Name, lib.ID)

		startIndex := 0
		const pageSize = 200
		for {
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
				if totalItems > maxScanItems {
					ls.logger.Warn("📚 [全库扫描] 达到安全阀上限 %d，停止扫描", maxScanItems)
					finishScan("安全阀")
					return
				}
				pending = append(pending, item)
				if len(pending) >= batchFlushSize {
					flushBatch()
				}
			}

			startIndex += pageSize
			if startIndex >= total || len(items) == 0 {
				break
			}

			if !ls.sleep(ctx) {
				finishScan("已停止")
				return
			}
		}
	}

	finishScan("")
}

// waitForAuth 等待认证信息就绪
func (ls *LibraryScanner) waitForAuth(ctx context.Context) (string, http.Header, bool) {
	const maxWait = 10 * time.Minute
	const checkInterval = 30 * time.Second
	deadline := time.Now().Add(maxWait)

	for {
		if ls.authStore.IsReady() {
			headers, userID, expired := ls.authStore.Get()
			if !expired && userID != "" && headers != nil {
				return userID, headers, true
			}
		}
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

// checkMemoryAndYield 内存安全检查
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
		select {
		case <-time.After(2 * time.Second):
		case <-ls.stopCh:
		case <-ctx.Done():
		}
	}
}

// doRequest 发送 HTTP 请求
func (ls *LibraryScanner) doRequest(ctx context.Context, authHeaders http.Header, path string) (*http.Response, error) {
	ls.server.proxyMu.RLock()
	targetURL := ls.server.targetURL
	ls.server.proxyMu.RUnlock()

	fullURL := targetURL.Scheme + "://" + targetURL.Host + path
	req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
	if err != nil {
		return nil, err
	}
	if authHeaders != nil {
		req.Header = authHeaders.Clone()
	}
	req.Header.Del("Accept-Encoding")
	req.Header.Set("Accept", "application/json")
	req.Host = targetURL.Host
	return ls.server.retryClient.Do(req)
}

// ==================== queryViews（已适配 FNOS） ====================
func (ls *LibraryScanner) queryViews(ctx context.Context, userID string, authHeaders http.Header) ([]libraryInfo, error) {
	path := "/v/api/v1/mediadb/list"

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

	var fnosResp fnosLibraryListResponse
	if err := json.Unmarshal(body, &fnosResp); err != nil {
		return nil, fmt.Errorf("JSON解析失败: %w", err)
	}

	if fnosResp.Code != 0 {
		return nil, fmt.Errorf("FNOS API 返回错误: code=%d, msg=%s", fnosResp.Code, fnosResp.Msg)
	}

	libraries := make([]libraryInfo, 0, len(fnosResp.Data))
	for _, item := range fnosResp.Data {
		if item.GUID == "" {
			continue
		}
		libraries = append(libraries, libraryInfo{
			ID:   item.GUID,
			Name: item.Title,
		})
	}
	return libraries, nil
}

// ==================== queryItems（已适配 FNOS） ====================
func (ls *LibraryScanner) queryItems(ctx context.Context, userID string, authHeaders http.Header, parentID string, startIndex, limit int) ([]PrefetchItem, int, error) {
	query := url.Values{}
	query.Set("libraryId", parentID)
	query.Set("start", strconv.Itoa(startIndex))
	query.Set("limit", strconv.Itoa(limit))
	path := "/v/api/v1/play/list?" + query.Encode()

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

	var fnosResp fnosPlayListResponse
	if err := json.Unmarshal(body, &fnosResp); err != nil {
		return nil, 0, fmt.Errorf("JSON解析失败: %w", err)
	}

	if fnosResp.Code != 0 {
		return nil, 0, fmt.Errorf("FNOS API 返回错误: code=%d, msg=%s", fnosResp.Code, fnosResp.Msg)
	}

	var result []PrefetchItem
	totalCount := len(fnosResp.Data)

	for _, item := range fnosResp.Data {
		if item.ID == "" {
			continue
		}
		result = append(result, PrefetchItem{
			ItemID: item.ID,
			UserID: userID,
			Name:   item.Name,
			Type:   item.Type,
		})
	}
	return result, totalCount, nil
}

// ==================== querySeasons（待完善） ====================
func (ls *LibraryScanner) querySeasons(ctx context.Context, userID string, authHeaders http.Header, seriesID string) ([]seasonInfo, error) {
	// ⚠️ 待修改：需要抓包确认 FNOS 获取季列表的 API
	path := "/emby/Shows/" + seriesID + "/Seasons"

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

// ==================== queryEpisodes（待完善） ====================
func (ls *LibraryScanner) queryEpisodes(ctx context.Context, userID string, authHeaders http.Header, seriesID, seasonID string) ([]PrefetchItem, error) {
	// ⚠️ 待修改：需要抓包确认 FNOS 获取集列表的 API
	query := url.Values{}
	query.Set("ParentId", seasonID)
	query.Set("fields", "ShareLevel,MediaSources")
	query.Set("SortBy", "IndexNumber")
	query.Set("SortOrder", "Ascending")
	path := "/emby/Users/" + userID + "/Items?" + query.Encode()

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

	var episodes []PrefetchItem
	for _, item := range listResp.Items {
		if item.Id == "" {
			continue
		}
		episodes = append(episodes, PrefetchItem{
			ItemID: item.Id,
			UserID: userID,
			Name:   item.Name,
			Type:   item.Type,
		})
	}
	return episodes, nil
}
