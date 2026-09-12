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

type libraryInfo struct {
	ID   string
	Name string
}

type seasonInfo struct {
	ID   string
	Name string
}

type jsonListResponse struct {
	Items            []jsonItem `json:"Items"`
	TotalRecordCount int        `json:"TotalRecordCount"`
}

type jsonItem struct {
	Id          string `json:"Id"`
	Name        string `json:"Name"`
	Type        string `json:"Type"`
	DateCreated string `json:"DateCreated"`
}

const maxScanItems = 10000
const batchFlushSize = 3
const memSafetyThresholdMB = 400

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

// Start 启动定时扫描
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
		ls.logger.Info("📚 [全库扫描] 定时任务已启动: %s", cron)
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
}

// Stop 停止扫描并等待所有 goroutine 退出
func (ls *LibraryScanner) Stop() {
	ls.stopOnce.Do(func() {
		close(ls.stopCh)
	})
	ls.wg.Wait()
}

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

// TriggerScan 手动触发（CAS 前置，避免并发双跑）
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
	status := map[string]interface{}{
		"running": ls.running.Load(),
		"lastScanStats": map[string]interface{}{
			"total":     ls.lastScanStats.Total,
			"success":   ls.lastScanStats.Success,
			"skipped":   ls.lastScanStats.Skipped,
			"deduped":   ls.lastScanStats.Deduped,
			"failed":    ls.lastScanStats.Failed,
			"cancelled": ls.lastScanStats.Cancelled,
		},
	}
	if !ls.lastScanTime.IsZero() {
		status["lastScanTime"] = ls.lastScanTime.Format("2006-01-02 15:04:05")
	}
	return status
}

// scanOnce 定时/启动触发入口（自持 running）
func (ls *LibraryScanner) scanOnce(ctx context.Context) {
	if !ls.running.CompareAndSwap(false, true) {
		ls.logger.Warn("📚 [全库扫描] 已有扫描在进行中，跳过")
		return
	}
	defer ls.running.Store(false)
	ls.scanOnceLocked(ctx)
}

// scanOnceLocked 已持有 running 的扫描主体
func (ls *LibraryScanner) scanOnceLocked(ctx context.Context) {
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
			ls.logger.Debug("📚 [全库扫描] 本批 %d 个，失败 %d 个，加入重试队列", len(pending), stats.Failed)
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
		label := "完成"
		if reason != "" {
			label = "结束(" + reason + ")"
		}
		ls.logger.Info("📚 [全库扫描] %s: 总计=%d 成功=%d 跳过=%d 去重=%d 失败=%d 取消=%d 耗时=%v",
			label, allStats.Total, allStats.Success, allStats.Skipped,
			allStats.Deduped, allStats.Failed, allStats.Cancelled, time.Since(startTime))
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

		items, total, err := ls.queryAllItems(ctx, userID, authHeaders, lib.ID)
		if err != nil {
			ls.logger.Warn("📚 [全库扫描] 查询项目失败: 库=%s err=%v", lib.Name, err)
			continue
		}
		ls.logger.Info("📚 [全库扫描] 库 %s 获取到 %d 个项目 (total=%d)", lib.Name, len(items), total)

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
	}

	finishScan("")
}

// waitForAuth 等待认证（去掉 IsReady 双重检查，避免竞态）
func (ls *LibraryScanner) waitForAuth(ctx context.Context) (string, http.Header, bool) {
	const maxWait = 10 * time.Minute
	const checkInterval = 30 * time.Second
	deadline := time.Now().Add(maxWait)

	for {
		headers, userID, expired := ls.authStore.Get()
		if !expired && userID != "" && headers != nil {
			return userID, headers, true
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

// checkMemoryAndYield 内存检查（改 HeapInuse + GC 节流）
func (ls *LibraryScanner) checkMemoryAndYield(ctx context.Context) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	memMB := float64(m.HeapInuse) / 1024 / 1024
	if memMB <= memSafetyThresholdMB {
		return
	}
	now := time.Now().Unix()
	last := ls.lastGCTime.Load()
	if now-last < 30 {
		return
	}
	ls.lastGCTime.Store(now)

	ls.logger.Warn("📚 [全库扫描] 内存过高 (%.1fMB/%dMB)，暂停扫描并触发GC", memMB, memSafetyThresholdMB)
	runtime.GC()
	debug.FreeOSMemory()
	select {
	case <-time.After(2 * time.Second):
	case <-ls.stopCh:
	case <-ctx.Done():
	}
}

// doRequest 保持原样
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

	extractToken := func(h http.Header) string {
		if h == nil {
			return ""
		}
		if auth := h.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
			return strings.TrimPrefix(auth, "Bearer ")
		}
		if token := h.Get("X-Emby-Token"); token != "" {
			return token
		}
		if embyAuth := h.Get("X-Emby-Authorization"); embyAuth != "" {
			if idx := strings.Index(embyAuth, `Token="`); idx >= 0 {
				rest := embyAuth[idx+7:]
				if end := strings.Index(rest, `"`); end > 0 {
					return rest[:end]
				}
			}
		}
		if auth := h.Get("Authorization"); auth != "" {
			return auth
		}
		return ""
	}

	token := extractToken(authHeaders)
	if token == "" {
		storedHeaders, _, _ := ls.authStore.Get()
		token = extractToken(storedHeaders)
	}

	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9")

	if token != "" {
		_, userID, _ := ls.authStore.Get()
		embyAuth := `MediaBrowser UserId="` + userID + `", Client="Emby Web", Device="Chrome", DeviceId="fnysfd-scanner", Version="4.7.0.0", Token="` + token + `"`
		req.Header.Set("X-Emby-Authorization", embyAuth)
		req.Header.Set("X-Emby-Token", token)
		preview := token
		if len(preview) > 8 {
			preview = preview[:8]
		}
		ls.logger.Debug("📤 [Emby请求] 已设置认证头 (Token: %s...)", preview)
	} else {
		ls.logger.Warn("⚠️ [Emby请求] 未找到 Token")
	}

	req.Header.Del("Accept-Encoding")
	req.Header.Set("Accept", "application/json")
	req.Host = targetURL.Host

	return ls.server.retryClient.Do(req)
}

// queryViews 空列表 fallback 简化
func (ls *LibraryScanner) queryViews(ctx context.Context, userID string, authHeaders http.Header) ([]libraryInfo, error) {
	path := "/emby/Users/" + userID + "/Views"

	resp, err := ls.doRequest(ctx, authHeaders, path)
	if err != nil {
		return nil, fmt.Errorf("请求失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("查询媒体库失败: status=%d, body=%s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 5*1024*1024))
	if err != nil {
		return nil, fmt.Errorf("读取响应体失败: %w", err)
	}

	var listResp jsonListResponse
	if err := json.Unmarshal(body, &listResp); err != nil {
		var items []jsonItem
		if err2 := json.Unmarshal(body, &items); err2 != nil {
			return nil, fmt.Errorf("JSON解析失败: %w / %w", err, err2)
		}
		listResp.Items = items
	}

	libraries := make([]libraryInfo, 0, len(listResp.Items))
	for _, item := range listResp.Items {
		if item.Id == "" {
			continue
		}
		libraries = append(libraries, libraryInfo{ID: item.Id, Name: item.Name})
	}
	return libraries, nil
}

// queryItems 单个分页查询（补齐 StartIndex）
func (ls *LibraryScanner) queryItems(ctx context.Context, userID string, authHeaders http.Header, parentID string, startIndex, limit int) ([]PrefetchItem, int, error) {
	if limit <= 0 {
		limit = 500
	}

	query := url.Values{}
	query.Set("ParentId", parentID)
	query.Set("Fields", "Path,MediaSources")
	query.Set("Limit", strconv.Itoa(limit))
	query.Set("StartIndex", strconv.Itoa(startIndex))

	path := "/emby/Users/" + userID + "/Items?" + query.Encode()

	resp, err := ls.doRequest(ctx, authHeaders, path)
	if err != nil {
		return nil, 0, fmt.Errorf("请求失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, 0, fmt.Errorf("查询项目失败: status=%d, body=%s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 50*1024*1024))
	if err != nil {
		return nil, 0, fmt.Errorf("读取响应体失败: %w", err)
	}

	var listResp jsonListResponse
	if err := json.Unmarshal(body, &listResp); err != nil {
		return nil, 0, fmt.Errorf("JSON解析失败: %w, body=%s", err, string(body))
	}

	var result []PrefetchItem
	for _, item := range listResp.Items {
		if item.Id == "" {
			continue
		}
		switch item.Type {
		case "Movie", "Video", "Episode":
			result = append(result, PrefetchItem{
				ItemID: item.Id,
				UserID: userID,
				Name:   item.Name,
				Type:   item.Type,
			})
		case "Series":
			select {
			case <-ls.stopCh:
				return result, listResp.TotalRecordCount, nil
			case <-ctx.Done():
				return result, listResp.TotalRecordCount, nil
			default:
			}
			episodes, err := ls.querySeriesEpisodes(ctx, userID, authHeaders, item.Id, item.Name)
			if err != nil {
				ls.logger.Warn("📚 [全库扫描] 展开剧集失败: %s err=%v", item.Name, err)
				continue
			}
			result = append(result, episodes...)
		default:
			ls.logger.Debug("📚 [全库扫描] 跳过未识别类型: %s (%s)", item.Name, item.Type)
		}
	}
	return result, listResp.TotalRecordCount, nil
}

// queryAllItems 分页拉取一个库的全部 Items
func (ls *LibraryScanner) queryAllItems(ctx context.Context, userID string, authHeaders http.Header, parentID string) ([]PrefetchItem, int, error) {
	const pageSize = 500
	var all []PrefetchItem
	total := 0
	for start := 0; ; start += pageSize {
		page, t, err := ls.queryItems(ctx, userID, authHeaders, parentID, start, pageSize)
		if err != nil {
			return all, total, err
		}
		total = t
		all = append(all, page...)

		if len(page) < pageSize || start+pageSize >= total {
			return all, total, nil
		}
		select {
		case <-ls.stopCh:
			return all, total, nil
		case <-ctx.Done():
			return all, total, nil
		default:
		}
	}
}

// querySeriesEpisodes 展开剧集（去掉固定 200ms sleep，交给 PrefetchBatch 节流）
func (ls *LibraryScanner) querySeriesEpisodes(ctx context.Context, userID string, authHeaders http.Header, seriesID string, seriesName string) ([]PrefetchItem, error) {
	path := "/emby/Shows/" + seriesID + "/Seasons"

	resp, err := ls.doRequest(ctx, authHeaders, path)
	if err != nil {
		return nil, fmt.Errorf("查询季列表请求失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("查询季列表失败: status=%d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 5*1024*1024))
	if err != nil {
		return nil, fmt.Errorf("读取季列表失败: %w", err)
	}

	var seasonsResp jsonListResponse
	if err := json.Unmarshal(body, &seasonsResp); err != nil {
		return nil, fmt.Errorf("解析季列表失败: %w", err)
	}

	if len(seasonsResp.Items) == 0 {
		return nil, nil
	}

	var allEpisodes []PrefetchItem
	for _, season := range seasonsResp.Items {
		if season.Id == "" {
			continue
		}
		select {
		case <-ls.stopCh:
			return allEpisodes, nil
		case <-ctx.Done():
			return allEpisodes, nil
		default:
		}
		episodes, err := ls.querySeasonEpisodes(ctx, userID, authHeaders, seriesID, season.Id)
		if err != nil {
			ls.logger.Warn("📚 [全库扫描] 查询季 %s 的集失败: %v", season.Name, err)
			continue
		}
		allEpisodes = append(allEpisodes, episodes...)
	}
	return allEpisodes, nil
}

func (ls *LibraryScanner) querySeasonEpisodes(ctx context.Context, userID string, authHeaders http.Header, seriesID, seasonID string) ([]PrefetchItem, error) {
	query := url.Values{}
	query.Set("SeasonId", seasonID)
	query.Set("Fields", "Path,MediaSources")
	query.Set("Limit", "500")

	path := "/emby/Shows/" + seriesID + "/Episodes?" + query.Encode()

	resp, err := ls.doRequest(ctx, authHeaders, path)
	if err != nil {
		return nil, fmt.Errorf("请求失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("查询集列表失败: status=%d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 50*1024*1024))
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

// ScanLibraryOnce 单库扫描（CAS 前置 + ctx.Err）
func (ls *LibraryScanner) ScanLibraryOnce(ctx context.Context, libID string) error {
	if !ls.running.CompareAndSwap(false, true) {
		return fmt.Errorf("已有扫描在进行中")
	}
	ls.wg.Add(1)
	defer ls.wg.Done()
	defer ls.running.Store(false)

	startTime := time.Now()

	userID, authHeaders, ok := ls.waitForAuth(ctx)
	if !ok {
		return fmt.Errorf("认证未就绪")
	}

	ls.logger.Info("📚 [单库扫描] 开始 (库=%s)", libID)

	items, total, err := ls.queryAllItems(ctx, userID, authHeaders, libID)
	if err != nil {
		return fmt.Errorf("查询项目失败: %w", err)
	}

	ls.logger.Info("📚 [单库扫描] 获取到 %d 个项目 (total=%d)", len(items), total)

	if len(items) == 0 {
		return nil
	}

	var allStats BatchStats
	var pending []PrefetchItem

	for _, item := range items {
		pending = append(pending, item)
		if len(pending) >= batchFlushSize {
			stats := ls.batch.PrefetchBatch(ctx, pending, authHeaders, "library_single", 3600, "单库扫描")
			allStats.Total += stats.Total
			allStats.Success += stats.Success
			allStats.Skipped += stats.Skipped
			allStats.Deduped += stats.Deduped
			allStats.Failed += stats.Failed
			allStats.Cancelled += stats.Cancelled
			pending = pending[:0]

			select {
			case <-time.After(800 * time.Millisecond):
			case <-ls.stopCh:
				return ctx.Err()
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	if len(pending) > 0 {
		stats := ls.batch.PrefetchBatch(ctx, pending, authHeaders, "library_single", 3600, "单库扫描")
		allStats.Total += stats.Total
		allStats.Success += stats.Success
		allStats.Skipped += stats.Skipped
		allStats.Deduped += stats.Deduped
		allStats.Failed += stats.Failed
		allStats.Cancelled += stats.Cancelled
	}

	ls.logger.Info("📚 [单库扫描] 完成: 总计=%d 成功=%d 跳过=%d 去重=%d 失败=%d 取消=%d 耗时=%v",
		allStats.Total, allStats.Success, allStats.Skipped, allStats.Deduped,
		allStats.Failed, allStats.Cancelled, time.Since(startTime))

	return nil
}
