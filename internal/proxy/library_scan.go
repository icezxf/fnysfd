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

// ========== 辅助结构体 ==========

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

// jsonListResponse Emby 列表 API 通用响应结构
type jsonListResponse struct {
	Items            []jsonItem `json:"Items"`
	TotalRecordCount int        `json:"TotalRecordCount"`
}

// jsonItem Emby Item 通用结构
type jsonItem struct {
	Id   string `json:"Id"`
	Name string `json:"Name"`
	Type string `json:"Type"`
}

// 常量
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

	// ✅ 失败的 item 收集起来，一轮扫完后重试
	var failedItems []PrefetchItem

	flushBatch := func() {
		if len(pending) == 0 {
			return
		}
		stats := ls.batch.PrefetchBatch(ctx, pending, authHeaders, "library", 3600, "全库扫描")
		allStats.Total += stats.Total
		allStats.Success += stats.Success
		allStats.Skipped += stats.Skipped
		allStats.Failed += stats.Failed

		// ✅ 收集失败项
		if stats.Failed > 0 {
			for _, item := range pending {
				failedItems = append(failedItems, item)
			}
			ls.logger.Debug("📚 [全库扫描] 本批 %d 个，失败 %d 个，加入重试队列", len(pending), stats.Failed)
		}

		pending = pending[:0]
		ls.checkMemoryAndYield(ctx)

		// ✅ 批次之间强制延迟，给飞牛 probe 喘息时间
		select {
		case <-time.After(800 * time.Millisecond):
		case <-ls.stopCh:
		case <-ctx.Done():
		}
	}

	// ✅ 重试所有失败项
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
			allStats.Success += stats.Success
			allStats.Failed += stats.Failed

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
		// ✅ 先重试失败项，再统计最终结果
		retryFailed()
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

		items, total, err := ls.queryItems(ctx, userID, authHeaders, lib.ID, 0, 500)
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

// checkMemoryAndYield 内存安全检查
func (ls *LibraryScanner) checkMemoryAndYield(ctx context.Context) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	memMB := float64(m.Alloc) / 1024 / 1024
	if memMB > memSafetyThresholdMB {
		ls.logger.Warn("📚 [全库扫描] 内存过高 (%.1fMB/%dMB)，暂停扫描并触发GC", memMB, memSafetyThresholdMB)
		runtime.GC()
		select {
		case <-time.After(2 * time.Second):
		case <-ls.stopCh:
		case <-ctx.Done():
		}
	}
}

// ============================================================
// ✅ doRequest：支持从 X-Emby-Authorization 解析 Token
// ============================================================
func (ls *LibraryScanner) doRequest(ctx context.Context, authHeaders http.Header, path string) (*http.Response, error) {
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

	// ✅ 提取 Token 的辅助函数（支持 4 种格式）
	extractToken := func(h http.Header) string {
		if h == nil {
			return ""
		}
		// 格式1: Authorization: Bearer xxx
		if auth := h.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
			return strings.TrimPrefix(auth, "Bearer ")
		}
		// 格式2: X-Emby-Token: xxx
		if token := h.Get("X-Emby-Token"); token != "" {
			return token
		}
		// 格式3: X-Emby-Authorization: MediaBrowser Client="...", Token="xxx"
		if embyAuth := h.Get("X-Emby-Authorization"); embyAuth != "" {
			if idx := strings.Index(embyAuth, `Token="`); idx >= 0 {
				rest := embyAuth[idx+7:]
				if end := strings.Index(rest, `"`); end > 0 {
					return rest[:end]
				}
			}
		}
		// 格式4: Authorization: xxx（无 Bearer 前缀，飞牛原生格式）
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

	// 补全 Emby 客户端头
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9")

	if token != "" {
		userID := ls.getUserID()
		embyAuth := `MediaBrowser UserId="` + userID + `", Client="Emby Web", Device="Chrome", DeviceId="fnysfd-scanner", Version="4.7.0.0", Token="` + token + `"`
		req.Header.Set("X-Emby-Authorization", embyAuth)
		req.Header.Set("X-Emby-Token", token)
		ls.logger.Debug("📤 [Emby请求] 已设置认证头 (Token: %s...)", token[:minInt(8, len(token))])
	} else {
		ls.logger.Warn("⚠️ [Emby请求] 未找到 Token")
	}

	req.Header.Del("Accept-Encoding")
	req.Header.Set("Accept", "application/json")
	req.Host = targetURL.Host

	return ls.server.retryClient.Do(req)
}

// getUserID 获取缓存的 UserID
func (ls *LibraryScanner) getUserID() string {
	_, userID, _ := ls.authStore.Get()
	return userID
}

// minInt 辅助函数
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ============================================================
// queryViews：查询媒体库列表
// ============================================================
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

	// 先尝试解析为标准 Emby 对象格式
	var listResp jsonListResponse
	if err := json.Unmarshal(body, &listResp); err == nil && len(listResp.Items) > 0 {
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

	// 再尝试解析为数组
	var items []jsonItem
	if err := json.Unmarshal(body, &items); err != nil {
		return nil, fmt.Errorf("JSON解析失败: %w, body=%s", err, string(body))
	}

	libraries := make([]libraryInfo, 0, len(items))
	for _, item := range items {
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

// ============================================================
// ✅ queryItems：使用已验证成功的参数组合
//    ParentId + Fields=Path,MediaSources + Limit
// ============================================================
func (ls *LibraryScanner) queryItems(ctx context.Context, userID string, authHeaders http.Header, parentID string, startIndex, limit int) ([]PrefetchItem, int, error) {
	_ = startIndex // 不使用（StartIndex 会导致返回空）

	if limit <= 0 {
		limit = 500
	}

	query := url.Values{}
	query.Set("ParentId", parentID)
	query.Set("Fields", "Path,MediaSources")
	query.Set("Limit", strconv.Itoa(limit))

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
		// 只处理 Movie 类型（Series 暂不展开）
		if item.Type != "Movie" {
			continue
		}
		result = append(result, PrefetchItem{
			ItemID: item.Id,
			UserID: userID,
			Name:   item.Name,
			Type:   item.Type,
		})
	}
	return result, listResp.TotalRecordCount, nil
}

// ============================================================
// querySeasons / queryEpisodes（暂未使用，保留代码）
// ============================================================

// querySeasons 查询季列表
func (ls *LibraryScanner) querySeasons(ctx context.Context, userID string, authHeaders http.Header, seriesID string) ([]seasonInfo, error) {
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

// queryEpisodes 查询集列表
func (ls *LibraryScanner) queryEpisodes(ctx context.Context, userID string, authHeaders http.Header, seriesID, seasonID string) ([]PrefetchItem, error) {
	query := url.Values{}
	query.Set("ParentId", seasonID)
	query.Set("Fields", "Path,MediaSources")
	query.Set("Limit", "500")
	path := "/emby/Users/" + userID + "/Items?" + query.Encode()

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
