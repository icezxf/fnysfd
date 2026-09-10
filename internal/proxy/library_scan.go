package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"fnysfd/internal/config"
	"fnysfd/internal/logger"
	"io"
	"net/http"
	"runtime"
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

func NewLibraryScanner(s *Server, b *BatchPrefetcher, auth *AuthStore) *LibraryScanner {
	return &LibraryScanner{
		server:    s,
		batch:     b,
		logger:    s.logger,
		authStore: auth,
		stopCh:    make(chan struct{}),
	}
}

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

func (ls *LibraryScanner) Stop() {
	ls.stopOnce.Do(func() {
		close(ls.stopCh)
	})
}

func (ls *LibraryScanner) TriggerScan() error {
	if ls.running.Load() {
		return fmt.Errorf("扫描正在进行中，请稍后再试")
	}
	go ls.scanOnce(context.Background())
	return nil
}

func (ls *LibraryScanner) IsRunning() bool {
	return ls.running.Load()
}

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

	// 遍历每个媒体库
	for _, lib := range libraries {
		select {
		case <-ls.stopCh:
			ls.logger.Info("📚 [全库扫描] 收到停止信号，中止扫描")
			finishScan("已停止")
			return
		default:
		}

		ls.logger.Info("📚 [全库扫描] 扫描媒体库: %s (ID=%s)", lib.Name, lib.ID)

		items, total, err := ls.queryItems(ctx, userID, authHeaders, lib.ID)
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
// ✅ doRequest：补全完整的 Emby 客户端头
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

	// 提取 Token
	token := ""
	if authHeaders != nil {
		if auth := authHeaders.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
			token = strings.TrimPrefix(auth, "Bearer ")
		}
		if token == "" {
			token = authHeaders.Get("X-Emby-Token")
		}
	}
	if token == "" {
		storedHeaders, _, _ := ls.authStore.Get()
		if storedHeaders != nil {
			if auth := storedHeaders.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
				token = strings.TrimPrefix(auth, "Bearer ")
			}
			if token == "" {
				token = storedHeaders.Get("X-Emby-Token")
			}
		}
	}

	// ✅ 补全 Emby 标准客户端头（模拟 Emby Web 客户端）
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36")
	req.Header.Set("X-Emby-Client", "Emby Web")
	req.Header.Set("X-Emby-Device-Name", "Chrome")
	req.Header.Set("X-Emby-Device-Id", "fnysfd-scanner")
	req.Header.Set("X-Emby-Client-Version", "4.7.0.0")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9")

	if token != "" {
		embyAuth := `MediaBrowser Client="Emby Web", Device="Chrome", DeviceId="fnysfd-scanner", Version="4.7.0.0", Token="` + token + `"`
		req.Header.Set("X-Emby-Authorization", embyAuth)
		req.Header.Set("X-Emby-Token", token)
		ls.logger.Debug("📤 [Emby请求] 已设置完整 Emby 客户端头 (Token: %s...)", token[:minInt(8, len(token))])
	} else {
		ls.logger.Warn("⚠️ [Emby请求] 未找到 Token")
	}

	req.Header.Del("Accept-Encoding")
	req.Header.Set("Accept", "application/json")
	req.Host = targetURL.Host

	return ls.server.retryClient.Do(req)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ============================================================
// queryViews：兼容数组或对象两种格式
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
// ✅ queryItems：带上 ParentId 参数（Emby /Items 端点要求）
// ============================================================
func (ls *LibraryScanner) queryItems(ctx context.Context, userID string, authHeaders http.Header, parentID string) ([]PrefetchItem, int, error) {
	// ✅ 带上 ParentId 参数（Emby API 的 /Items 端点要求）
	path := "/emby/Users/" + userID + "/Items?ParentId=" + parentID + "&Recursive=true"

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
	var result []PrefetchItem
	var totalCount int

	if err := json.Unmarshal(body, &listResp); err == nil && len(listResp.Items) > 0 {
		totalCount = listResp.TotalRecordCount
		for _, item := range listResp.Items {
			if item.Id == "" {
				continue
			}
			if item.Type != "Movie" && item.Type != "Series" {
				continue
			}
			result = append(result, PrefetchItem{
				ItemID: item.Id,
				UserID: userID,
				Name:   item.Name,
				Type:   item.Type,
			})
		}
		return result, totalCount, nil
	}

	var items []jsonItem
	if err := json.Unmarshal(body, &items); err != nil {
		return nil, 0, fmt.Errorf("JSON解析失败: %w, body=%s", err, string(body))
	}

	for _, item := range items {
		if item.Id == "" {
			continue
		}
		if item.Type != "Movie" && item.Type != "Series" {
			continue
		}
		result = append(result, PrefetchItem{
			ItemID: item.Id,
			UserID: userID,
			Name:   item.Name,
			Type:   item.Type,
		})
	}
	return result, len(items), nil
}
