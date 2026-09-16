package dashboard

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fnysfd/internal/cache"
	"fnysfd/internal/config"
	"fnysfd/internal/docker"
	"fnysfd/internal/handler"
	"fnysfd/internal/logger"
	"fnysfd/internal/proxy" // ✅ 新增：观看记录服务类型
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Dashboard 管理面板
type Dashboard struct {
	config         *config.Config
	cache          *cache.Cache
	logger         *logger.Logger
	streamHandler  *handler.StreamHandler
	libraryScanner LibraryScannerInterface
	doubanProvider DoubanProviderInterface
	recordProvider *proxy.RecordService // ✅ 观看记录服务（nil=禁用）
	startTime      time.Time
	sessions       map[string]session
	sessionMutex   sync.RWMutex
	server         *http.Server
	version        string
	stopChan       chan struct{}
	stopOnce       sync.Once
}

// LibraryScannerInterface 全库扫描器接口
type LibraryScannerInterface interface {
	TriggerScan() error
	IsRunning() bool
	GetStatus() map[string]interface{}
}

// DoubanProviderInterface 豆瓣评分提供器接口
type DoubanProviderInterface interface {
	GetStats() map[string]interface{}
	ListCache() []map[string]interface{}
	DeleteCacheEntry(keys []string) int
	ClearCache() int
}

type session struct {
	username  string
	createdAt time.Time
}

// APIResponse 通用API响应结构
type APIResponse struct {
	Code    int         `json:"code"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
	Logs    []string    `json:"logs,omitempty"`
}

// SystemInfo 系统信息结构
type SystemInfo struct {
	Version       string  `json:"version"`
	GoVersion     string  `json:"go_version"`
	OS            string  `json:"os"`
	Arch          string  `json:"arch"`
	NumCPU        int     `json:"num_cpu"`
	Goroutines    int     `json:"goroutines"`
	MemoryUsageMB float64 `json:"memory_usage_mb"`
	UptimeSeconds float64 `json:"uptime_seconds"`
	StartTime     string  `json:"start_time"`
}

// New 创建管理面板
func New(cfg *config.Config, c *cache.Cache, l *logger.Logger, version string) *Dashboard {
	if version == "" {
		version = "dev"
	}
	return &Dashboard{
		config:    cfg,
		cache:     c,
		logger:    l,
		startTime: time.Now(),
		sessions:  make(map[string]session),
		version:   version,
		stopChan:  make(chan struct{}),
	}
}

// writeJSON 统一的 JSON 响应辅助方法
func (d *Dashboard) writeJSON(w http.ResponseWriter, code int, resp APIResponse) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		d.logger.Error("❌ JSON 响应编码失败: %v", err)
	}
}

// SetStreamHandler 设置StreamHandler引用
func (d *Dashboard) SetStreamHandler(sh *handler.StreamHandler) {
	d.streamHandler = sh
}

// SetLibraryScanner 设置全库扫描器引用
func (d *Dashboard) SetLibraryScanner(ls LibraryScannerInterface) {
	d.libraryScanner = ls
}

// SetDoubanProvider 设置豆瓣评分提供器引用
func (d *Dashboard) SetDoubanProvider(p DoubanProviderInterface) {
	d.doubanProvider = p
}

// ✅ SetRecordProvider 设置观看记录服务引用
func (d *Dashboard) SetRecordProvider(rs *proxy.RecordService) {
	d.recordProvider = rs
}

// Start 启动面板服务
func (d *Dashboard) Start() error {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/login", d.csrfMiddleware(d.handleLogin))
	mux.HandleFunc("/api/logout", d.authMiddleware(d.csrfMiddleware(d.handleLogout)))
	mux.HandleFunc("/login", d.showLoginPage)
	mux.HandleFunc("/api/stats", d.authMiddleware(d.handleStats))
	mux.HandleFunc("/api/cache/clear", d.authMiddleware(d.csrfMiddleware(d.handleCacheClear)))
	mux.HandleFunc("/api/logs/clear", d.authMiddleware(d.csrfMiddleware(d.handleLogsClear)))
	mux.HandleFunc("/api/system", d.authMiddleware(d.handleSystem))
	mux.HandleFunc("/api/config/get", d.authMiddleware(d.handleConfigGet))
	mux.HandleFunc("/api/config/update", d.authMiddleware(d.csrfMiddleware(d.handleConfigUpdate)))
	mux.HandleFunc("/api/strm_paths/get", d.authMiddleware(d.handleStrmPathsGet))
	mux.HandleFunc("/api/strm_paths/add", d.authMiddleware(d.csrfMiddleware(d.handleStrmPathAdd)))
	mux.HandleFunc("/api/strm_paths/delete", d.authMiddleware(d.csrfMiddleware(d.handleStrmPathDelete)))
	mux.HandleFunc("/api/strm_paths/volumes", d.authMiddleware(d.handleStrmPathsVolumes))
	mux.HandleFunc("/api/docker/restart", d.authMiddleware(d.csrfMiddleware(d.handleDockerRestart)))
	mux.HandleFunc("/api/logs", d.authMiddleware(d.handleLogs))
	mux.HandleFunc("/api/scan/trigger", d.authMiddleware(d.csrfMiddleware(d.handleScanTrigger)))
	mux.HandleFunc("/api/scan/status", d.authMiddleware(d.handleScanStatus))
	mux.HandleFunc("/api/scan/notify", d.handleScanNotify)
	// 豆瓣缓存管理
	mux.HandleFunc("/api/douban/cache", d.authMiddleware(d.handleDoubanCache))
	mux.HandleFunc("/api/douban/cache/delete", d.authMiddleware(d.csrfMiddleware(d.handleDoubanCacheDelete)))
	mux.HandleFunc("/api/douban/cache/clear", d.authMiddleware(d.csrfMiddleware(d.handleDoubanCacheClear)))
	// ✅ 观看记录
	mux.HandleFunc("/api/record/stats", d.authMiddleware(d.handleRecordStats))
	mux.HandleFunc("/api/record/users", d.authMiddleware(d.handleRecordUsers))
	mux.HandleFunc("/api/record/history", d.authMiddleware(d.handleRecordHistory))

	mux.HandleFunc("/", d.authMiddleware(d.handleIndex))

	d.server = &http.Server{
		Addr:         d.config.GetDashboardAddr(),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	d.logger.Info("🌐 管理面板启动: http://localhost%s", d.config.GetDashboardAddr())

	go func() {
		if err := d.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			d.logger.Error("管理面板启动失败: %v", err)
		}
	}()

	go d.cleanupSessionsLoop()

	return nil
}

// cleanupSessionsLoop 定期清理过期 session
func (d *Dashboard) cleanupSessionsLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			d.cleanupExpiredSessions()
		case <-d.stopChan:
			d.logger.Info("🛑 session 清理协程已停止")
			return
		}
	}
}

func (d *Dashboard) cleanupExpiredSessions() {
	d.sessionMutex.Lock()
	defer d.sessionMutex.Unlock()
	removed := 0
	for id, s := range d.sessions {
		if time.Since(s.createdAt) > 24*time.Hour {
			delete(d.sessions, id)
			removed++
		}
	}
	if removed > 0 {
		d.logger.Info("🧹 清理过期 session: %d 个", removed)
	}
}

// Stop 停止面板服务
func (d *Dashboard) Stop() error {
	d.stopOnce.Do(func() {
		close(d.stopChan)
	})
	if d.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return d.server.Shutdown(ctx)
	}
	return nil
}

// authMiddleware 认证中间件
func (d *Dashboard) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie("session_id")
		if err != nil {
			if strings.HasPrefix(r.URL.Path, "/api/") {
				d.writeJSON(w, http.StatusUnauthorized, APIResponse{Code: 401, Message: "未登录或会话已过期"})
				return
			}
			http.Redirect(w, r, "/login", http.StatusFound)
			return
		}

		d.sessionMutex.RLock()
		sess, exists := d.sessions[cookie.Value]
		d.sessionMutex.RUnlock()

		if !exists || time.Since(sess.createdAt) > 24*time.Hour {
			if strings.HasPrefix(r.URL.Path, "/api/") {
				d.writeJSON(w, http.StatusUnauthorized, APIResponse{Code: 401, Message: "会话已过期，请重新登录"})
				return
			}
			http.Redirect(w, r, "/login", http.StatusFound)
			return
		}

		d.sessionMutex.Lock()
		if cur, ok := d.sessions[cookie.Value]; ok && time.Since(cur.createdAt) <= 24*time.Hour {
			cur.createdAt = time.Now()
			d.sessions[cookie.Value] = cur
		}
		d.sessionMutex.Unlock()

		next(w, r)
	}
}

// csrfMiddleware CSRF 防护中间件
func (d *Dashboard) csrfMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" && r.Method != "HEAD" {
			if r.Header.Get("X-Requested-With") != "XMLHttpRequest" {
				d.writeJSON(w, http.StatusForbidden, APIResponse{Code: 403, Message: "CSRF 校验失败：缺少必要的请求头"})
				return
			}
		}
		next(w, r)
	}
}

func generateSessionID() string {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		now := time.Now().UnixNano()
		for i := 0; i < 32; i++ {
			bytes[i] = byte(now >> (i % 8 * 8))
		}
	}
	return hex.EncodeToString(bytes)
}

// handleLogin 处理登录
func (d *Dashboard) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		http.Redirect(w, r, "/login", http.StatusFound)
		return
	}
	if r.Method != "POST" {
		d.writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Code: 405, Message: "方法不允许"})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 4*1024)

	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 400, Message: "请求格式错误"})
		return
	}

	expectedUser, expectedPass := d.config.GetDashboardCredentials()

	userOK := subtle.ConstantTimeCompare([]byte(req.Username), []byte(expectedUser)) == 1
	passOK := subtle.ConstantTimeCompare([]byte(req.Password), []byte(expectedPass)) == 1

	if userOK && passOK {
		sessionID := generateSessionID()

		d.sessionMutex.Lock()
		d.sessions[sessionID] = session{
			username:  req.Username,
			createdAt: time.Now(),
		}
		d.sessionMutex.Unlock()

		cookie := &http.Cookie{
			Name:     "session_id",
			Value:    sessionID,
			Path:     "/",
			MaxAge:   86400,
			HttpOnly: true,
			SameSite: http.SameSiteLaxMode,
			Secure:   r.TLS != nil,
		}
		http.SetCookie(w, cookie)

		d.writeJSON(w, http.StatusOK, APIResponse{Code: 200, Message: "登录成功"})
	} else {
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 401, Message: "用户名或密码错误"})
	}
}

// handleLogout 处理登出
func (d *Dashboard) handleLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		d.writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Code: 405, Message: "方法不允许"})
		return
	}
	cookie, err := r.Cookie("session_id")
	if err == nil {
		d.sessionMutex.Lock()
		delete(d.sessions, cookie.Value)
		d.sessionMutex.Unlock()

		cookie.MaxAge = -1
		cookie.Secure = r.TLS != nil
		http.SetCookie(w, cookie)
	}

	d.writeJSON(w, http.StatusOK, APIResponse{Code: 200, Message: "已登出"})
}

// showLoginPage 显示登录页面
func (d *Dashboard) showLoginPage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(loginHTML))
}

// handleIndex 首页
func (d *Dashboard) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(indexHTML))
}

// handleStats 获取统计信息
func (d *Dashboard) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		d.writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Code: 405, Message: "方法不允许"})
		return
	}

	if d.streamHandler != nil {
		d.cache.SetExternalCacheCounts(
			d.streamHandler.GetStrmCacheSize(),
			d.streamHandler.GetURLCacheSize(),
		)
	}

	stats := d.cache.GetStats()

	data := map[string]interface{}{
		"media_source_count": stats.MediaSourceCount,
		"stream_url_count":   stats.StreamURLCount,
		"strm_cache_count":   stats.StrmCacheCount,
		"url_cache_count":    stats.URLCacheCount,
		"memory_usage_mb":    stats.MemoryUsageMB,
		"hit_count":          stats.HitCount,
		"miss_count":         stats.MissCount,
		"hit_rate":           stats.HitRate,
		"evicted_count":      stats.EvictedCount,
	}

	if d.streamHandler != nil {
		for k, v := range d.streamHandler.GetStats() {
			data[k] = v
		}
	}

	if d.doubanProvider != nil {
		ds := d.doubanProvider.GetStats()
		data["douban_enabled"] = ds["enabled"]
		data["douban_entries"] = ds["entries"]
		data["douban_file_size_kb"] = ds["file_size_kb"]
		data["douban_updated_at"] = ds["updated_at"]
		data["douban_stat_hits"] = ds["stat_hits"]
		data["douban_stat_misses"] = ds["stat_misses"]
		data["douban_stat_fetched"] = ds["stat_fetched"]
		data["douban_stat_failed"] = ds["stat_failed"]
	}

	d.writeJSON(w, http.StatusOK, APIResponse{Code: 200, Data: data})
}

// ============================================================
// 豆瓣缓存管理
// ============================================================

func (d *Dashboard) handleDoubanCache(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		d.writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Code: 405, Message: "方法不允许"})
		return
	}
	if d.doubanProvider == nil {
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 200, Data: []interface{}{}})
		return
	}
	items := d.doubanProvider.ListCache()
	d.writeJSON(w, http.StatusOK, APIResponse{Code: 200, Data: items})
}

func (d *Dashboard) handleDoubanCacheDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		d.writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Code: 405, Message: "方法不允许"})
		return
	}
	if d.doubanProvider == nil {
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 500, Message: "豆瓣提供器未初始化"})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 8*1024)
	var req struct {
		Keys []string `json:"keys"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 400, Message: "请求格式错误"})
		return
	}

	n := d.doubanProvider.DeleteCacheEntry(req.Keys)
	d.logger.Info("🎬 面板删除豆瓣缓存: %d 条", n)
	d.writeJSON(w, http.StatusOK, APIResponse{Code: 200, Message: fmt.Sprintf("已删除 %d 条缓存", n)})
}

func (d *Dashboard) handleDoubanCacheClear(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		d.writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Code: 405, Message: "方法不允许"})
		return
	}
	if d.doubanProvider == nil {
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 500, Message: "豆瓣提供器未初始化"})
		return
	}

	n := d.doubanProvider.ClearCache()
	d.logger.Info("🎬 面板清空豆瓣缓存: %d 条", n)
	d.writeJSON(w, http.StatusOK, APIResponse{Code: 200, Message: fmt.Sprintf("已清空 %d 条缓存", n)})
}

// ============================================================
// ✅ 观看记录
// ============================================================

// handleRecordStats 统计数据
func (d *Dashboard) handleRecordStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		d.writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Code: 405, Message: "方法不允许"})
		return
	}
	if d.recordProvider == nil || !d.recordProvider.Available() {
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 200, Data: map[string]interface{}{
			"total_users": 0, "total_plays": 0, "active_users": 0, "today_plays": 0, "latest_play": "",
		}})
		return
	}
	stats, err := d.recordProvider.GetStats()
	if err != nil {
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 500, Message: err.Error()})
		return
	}
	d.writeJSON(w, http.StatusOK, APIResponse{Code: 200, Data: stats})
}

// handleRecordUsers 用户列表
func (d *Dashboard) handleRecordUsers(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		d.writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Code: 405, Message: "方法不允许"})
		return
	}
	if d.recordProvider == nil || !d.recordProvider.Available() {
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 200, Data: []interface{}{}})
		return
	}
	users, err := d.recordProvider.GetUsers()
	if err != nil {
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 500, Message: err.Error()})
		return
	}
	d.writeJSON(w, http.StatusOK, APIResponse{Code: 200, Data: users})
}

// handleRecordHistory 播放历史
func (d *Dashboard) handleRecordHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		d.writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Code: 405, Message: "方法不允许"})
		return
	}
	if d.recordProvider == nil || !d.recordProvider.Available() {
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 200, Data: map[string]interface{}{
			"total": 0, "page": 1, "per_page": 20, "pages": 0, "data": []interface{}{},
		}})
		return
	}

	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	perPage, _ := strconv.Atoi(r.URL.Query().Get("per_page"))
	if perPage < 1 || perPage > 200 {
		perPage = 20
	}

	q := proxy.HistoryQuery{
		UserGUID:    r.URL.Query().Get("user_guid"),
		Page:        page,
		PerPage:     perPage,
		SearchTitle: strings.TrimSpace(r.URL.Query().Get("search_title")),
		StartTime:   r.URL.Query().Get("start_time"),
		EndTime:     r.URL.Query().Get("end_time"),
	}

	result, err := d.recordProvider.GetHistory(q)
	if err != nil {
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 500, Message: err.Error()})
		return
	}
	d.writeJSON(w, http.StatusOK, APIResponse{Code: 200, Data: result})
}

// ============================================================
// 其他 handler（清缓存、日志、路径管理、扫描、Docker 等）
// ============================================================

// handleCacheClear 清空缓存
func (d *Dashboard) handleCacheClear(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		d.writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Code: 405, Message: "方法不允许"})
		return
	}

	d.cache.Clear()

	if d.streamHandler != nil {
		d.streamHandler.ClearExternalCaches()
	}

	d.logger.Info("🗑️ 通过管理面板清空缓存")
	d.writeJSON(w, http.StatusOK, APIResponse{Code: 200, Message: "缓存已清空"})
}

// handleLogsClear 清空日志文件
func (d *Dashboard) handleLogsClear(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		d.writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Code: 405, Message: "方法不允许"})
		return
	}

	logDir := d.config.GetLogDir()
	if logDir == "" {
		logDir = "./logs"
	}

	files, err := filepath.Glob(filepath.Join(logDir, "*.log"))
	if err != nil {
		d.logger.Error("❌ 读取日志目录失败: %v", err)
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 500, Message: "读取日志目录失败: " + err.Error()})
		return
	}

	deletedCount := 0
	skippedCount := 0
	for _, file := range files {
		base := filepath.Base(file)
		if !isAppLogFile(base) {
			skippedCount++
			continue
		}
		if err := os.Remove(file); err != nil {
			d.logger.Warn("⚠️ 删除日志文件失败: %s, Error=%v", file, err)
			continue
		}
		deletedCount++
		d.logger.Info("🗑️ 已删除日志文件: %s", base)
	}

	d.writeJSON(w, http.StatusOK, APIResponse{
		Code:    200,
		Message: fmt.Sprintf("已清除 %d 个日志文件", deletedCount),
		Data:    map[string]int{"deleted_count": deletedCount, "skipped_count": skippedCount},
	})
}

func isAppLogFile(name string) bool {
	stem := strings.TrimSuffix(name, ".log")
	if stem == name {
		return false
	}
	if len(stem) == 8 {
		return isAllDigits(stem)
	}
	if idx := strings.IndexByte(stem, '_'); idx == 8 && len(stem) > 9 {
		return isAllDigits(stem[:8]) && isAllDigits(stem[9:])
	}
	return false
}

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// handleSystem 获取系统信息
func (d *Dashboard) handleSystem(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		d.writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Code: 405, Message: "方法不允许"})
		return
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	info := SystemInfo{
		Version:       d.version,
		GoVersion:     runtime.Version(),
		OS:            runtime.GOOS,
		Arch:          runtime.GOARCH,
		NumCPU:        runtime.NumCPU(),
		Goroutines:    runtime.NumGoroutine(),
		MemoryUsageMB: float64(m.Sys) / 1024 / 1024,
		UptimeSeconds: time.Since(d.startTime).Seconds(),
		StartTime:     d.startTime.Format("2006-01-02 15:04:05"),
	}

	d.writeJSON(w, http.StatusOK, APIResponse{Code: 200, Data: info})
}

// handleConfigGet 获取配置
func (d *Dashboard) handleConfigGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		d.writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Code: 405, Message: "方法不允许"})
		return
	}
	cfg := d.config.ToMap()
	d.writeJSON(w, http.StatusOK, APIResponse{Code: 200, Data: cfg})
}

// handleConfigUpdate 更新配置
func (d *Dashboard) handleConfigUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		d.writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Code: 405, Message: "方法不允许"})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 16*1024)

	var updates map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 400, Message: "请求格式错误"})
		return
	}

	needsRestart, err := d.config.UpdateConfigWithStatus(updates)
	if err != nil {
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 500, Message: "更新失败: " + err.Error()})
		return
	}

	d.logger.Info("⚙️ 配置已通过面板更新")

	dataMap := map[string]interface{}{
		"needs_restart": needsRestart,
		"message":       "",
	}
	if needsRestart {
		dataMap["message"] = "检测到需要重启的配置变更，请点击'重启容器'按钮或执行 docker compose restart"
	}

	d.writeJSON(w, http.StatusOK, APIResponse{
		Code:    200,
		Message: "配置更新成功",
		Data:    dataMap,
	})
}

// handleLogs 获取日志
func (d *Dashboard) handleLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		d.writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Code: 405, Message: "方法不允许"})
		return
	}

	limit := 200
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			limit = n
			if limit > 2000 {
				limit = 2000
			}
		}
	}

	logDir := d.config.GetLogDir()
	if logDir == "" {
		logDir = "./logs"
	}

	today := time.Now().Format("20060102")
	logFile := filepath.Join(logDir, today+".log")

	if _, statErr := os.Stat(logFile); statErr != nil {
		files, _ := filepath.Glob(filepath.Join(logDir, "*.log"))
		if len(files) > 0 {
			sort.Strings(files)
			logFile = files[len(files)-1]
		} else {
			d.writeJSON(w, http.StatusOK, APIResponse{Code: 200, Logs: []string{"暂无日志文件"}})
			return
		}
	}

	lines, err := readLastLines(logFile, limit)
	if err != nil {
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 200, Logs: []string{"无法读取日志文件: " + err.Error()}})
		return
	}

	if len(lines) == 0 {
		lines = []string{"暂无日志内容"}
	}

	d.writeJSON(w, http.StatusOK, APIResponse{Code: 200, Logs: lines})
}

// readLastLines 读取文件的最后 limit 行
func readLastLines(path string, limit int) ([]string, error) {
	const maxTailBytes int64 = 1 * 1024 * 1024

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	if info.Size() > maxTailBytes {
		if _, seekErr := file.Seek(-maxTailBytes, io.SeekEnd); seekErr != nil {
			return nil, seekErr
		}
		reader := bufio.NewReader(file)
		if _, err := reader.ReadBytes('\n'); err != nil && err != io.EOF {
			return nil, err
		}
		var ring []string
		scanner := bufio.NewScanner(reader)
		scanner.Buffer(make([]byte, 64*1024), 1024*1024)
		for scanner.Scan() {
			ring = append(ring, scanner.Text())
			if len(ring) > limit {
				ring = ring[1:]
			}
		}
		if scanErr := scanner.Err(); scanErr != nil {
			return nil, scanErr
		}
		return ring, nil
	}

	var all []string
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		all = append(all, scanner.Text())
	}
	if scanErr := scanner.Err(); scanErr != nil {
		return nil, scanErr
	}
	if len(all) > limit {
		all = all[len(all)-limit:]
	}
	return all, nil
}

// handleStrmPathsGet 获取STRM路径列表
func (d *Dashboard) handleStrmPathsGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		d.writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Code: 405, Message: "方法不允许"})
		return
	}

	paths := d.config.GetStrmPaths()
	volumes := d.config.GetStrmVolumes()
	volumeDetails := d.config.GenerateDockerVolumes()

	d.writeJSON(w, http.StatusOK, APIResponse{
		Code: 200,
		Data: map[string]interface{}{
			"strm_paths":   paths,
			"strm_volumes": volumes,
			"volumes":      volumeDetails,
			"count":        len(volumes),
		},
	})
}

// handleStrmPathAdd 添加STRM路径
func (d *Dashboard) handleStrmPathAdd(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		d.writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Code: 405, Message: "方法不允许"})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 4*1024)

	var req struct {
		HostPath      string `json:"host_path"`
		ContainerPath string `json:"container_path"`
	}

	bodyBytes, _ := io.ReadAll(r.Body)

	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		d.logger.Error("❌ JSON解析失败: %v", err)
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 400, Message: fmt.Sprintf("JSON格式错误: %v", err)})
		return
	}

	if err := validatePath(req.HostPath, "主机路径"); err != nil {
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 400, Message: err.Error()})
		return
	}
	if err := validatePath(req.ContainerPath, "容器路径"); err != nil {
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 400, Message: err.Error()})
		return
	}

	availableNow, err := d.config.AddStrmVolume(req.HostPath, req.ContainerPath)
	if err != nil {
		d.logger.Error("❌ 添加STRM Volume失败: %s:%s, Error=%v", req.HostPath, req.ContainerPath, err)
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 400, Message: err.Error()})
		return
	}

	volumeFormat := req.HostPath + ":" + req.ContainerPath + ":ro"

	composeUpdated := false
	if err := d.config.UpdateComposeFile(); err != nil {
		d.logger.Warn("⚠️  Compose文件更新失败（不影响功能）: %v", err)
	} else {
		composeUpdated = true
	}

	msg := "Volume映射添加成功"
	if availableNow {
		msg = "Volume映射添加成功，已立即生效（无需重建容器）"
	} else {
		msg = "Volume映射添加成功，路径在容器内暂不可访问，建议重启容器生效"
	}

	d.writeJSON(w, http.StatusOK, APIResponse{
		Code:    200,
		Message: msg,
		Data: map[string]interface{}{
			"volume_format":   volumeFormat,
			"host_path":       req.HostPath,
			"container_path":  req.ContainerPath,
			"strm_volumes":    d.config.GetStrmVolumes(),
			"strm_paths":      d.config.GetStrmPaths(),
			"compose_updated": composeUpdated,
			"available_now":   availableNow,
			"needs_restart":   !availableNow,
		},
	})
}

// validatePath 校验路径安全性
func validatePath(path, label string) error {
	p := strings.TrimSpace(path)
	if p == "" {
		return fmt.Errorf("%s不能为空", label)
	}
	if !strings.HasPrefix(p, "/") {
		return fmt.Errorf("%s必须以 / 开头", label)
	}
	cleaned := filepath.Clean(p)
	for _, seg := range strings.Split(cleaned, string(filepath.Separator)) {
		if seg == ".." {
			return fmt.Errorf("%s不允许包含 '..' 路径段", label)
		}
	}
	systemPaths := []string{"/etc", "/proc", "/sys", "/dev", "/boot"}
	for _, sp := range systemPaths {
		if cleaned == sp || strings.HasPrefix(cleaned, sp+"/") {
			return fmt.Errorf("%s不允许使用系统关键路径: %s", label, sp)
		}
	}
	return nil
}

// handleStrmPathDelete 删除STRM路径
func (d *Dashboard) handleStrmPathDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		d.writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Code: 405, Message: "方法不允许"})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 4*1024)

	var req struct {
		VolumeFormat string `json:"volume_format"`
	}

	bodyBytes, _ := io.ReadAll(r.Body)

	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		d.logger.Error("❌ JSON解析失败: %v", err)
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 400, Message: fmt.Sprintf("JSON格式错误: %v", err)})
		return
	}

	if err := d.config.DeleteStrmVolume(req.VolumeFormat); err != nil {
		d.logger.Error("❌ 删除STRM Volume失败: %s, Error=%v", req.VolumeFormat, err)
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 400, Message: err.Error()})
		return
	}

	d.logger.Info("✅ 成功删除STRM Volume: %s", req.VolumeFormat)

	d.writeJSON(w, http.StatusOK, APIResponse{
		Code:    200,
		Message: "Volume映射删除成功",
		Data: map[string]interface{}{
			"volume_format": req.VolumeFormat,
			"strm_volumes":  d.config.GetStrmVolumes(),
			"strm_paths":    d.config.GetStrmPaths(),
		},
	})
}

// handleStrmPathsVolumes 获取Docker Volumes配置建议
func (d *Dashboard) handleStrmPathsVolumes(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		d.writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Code: 405, Message: "方法不允许"})
		return
	}

	paths := d.config.GetStrmPaths()
	volumes := d.config.GetStrmVolumes()
	volumeDetails := d.config.GenerateDockerVolumes()

	d.writeJSON(w, http.StatusOK, APIResponse{
		Code: 200,
		Data: map[string]interface{}{
			"strm_paths":   paths,
			"strm_volumes": volumes,
			"volumes":      volumeDetails,
			"count":        len(volumes),
			"compose_yaml": generateComposeYAML(volumeDetails),
		},
	})
}

// generateComposeYAML 生成完整的docker-compose volumes片段
func generateComposeYAML(volumes []map[string]string) string {
	if len(volumes) == 0 {
		return "# 暂无STRM Volume配置\n# 请在面板中添加STRM路径映射"
	}

	var yaml strings.Builder
	yaml.WriteString("# Docker Compose - Volumes 配置（自动生成）\n")
	yaml.WriteString("volumes:\n")

	for i, vol := range volumes {
		yaml.WriteString(fmt.Sprintf("  # %d. %s\n", i+1, vol["description"]))
		yaml.WriteString(fmt.Sprintf("  - %s\n", vol["volume_format"]))
	}

	return yaml.String()
}

// handleDockerRestart 一键重启容器
func (d *Dashboard) handleDockerRestart(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		d.writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Code: 405, Message: "方法不允许"})
		return
	}

	if docker.Global == nil || !docker.Global.IsEnabled() {
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 500, Message: "Docker 管理器未启用，无法重启容器"})
		return
	}

	d.logger.Info("🔄 面板触发容器重启...")

	if err := docker.Global.RestartContainer(); err != nil {
		d.logger.Error("❌ 容器重启失败: %v", err)
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 500, Message: "容器重启失败: " + err.Error()})
		return
	}

	d.writeJSON(w, http.StatusOK, APIResponse{
		Code:    200,
		Message: "容器重启命令已发送，请等待约 5-10 秒后刷新页面",
	})
}

// handleScanTrigger 手动触发全库扫描
func (d *Dashboard) handleScanTrigger(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		d.writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Code: 405, Message: "方法不允许"})
		return
	}

	if d.libraryScanner == nil {
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 500, Message: "全库扫描器未初始化"})
		return
	}

	if err := d.libraryScanner.TriggerScan(); err != nil {
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 409, Message: err.Error()})
		return
	}

	d.logger.Info("📚 面板触发全库扫描")
	d.writeJSON(w, http.StatusOK, APIResponse{
		Code:    200,
		Message: "全库扫描已启动，请稍后查看状态",
	})
}

// handleScanStatus 获取全库扫描状态
func (d *Dashboard) handleScanStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		d.writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Code: 405, Message: "方法不允许"})
		return
	}

	if d.libraryScanner == nil {
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 200, Data: map[string]interface{}{
			"running":       false,
			"lastScanTime":  "",
			"lastScanStats": map[string]interface{}{},
		}})
		return
	}

	d.writeJSON(w, http.StatusOK, APIResponse{Code: 200, Data: d.libraryScanner.GetStatus()})
}

// handleScanNotify Webhook 端点
func (d *Dashboard) handleScanNotify(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		d.writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Code: 405, Message: "方法不允许"})
		return
	}

	expectedToken := d.config.GetWebhookNotifyToken()
	if expectedToken == "" {
		d.writeJSON(w, http.StatusForbidden, APIResponse{Code: 403, Message: "服务端未配置 webhook_notify_token"})
		return
	}

	token := r.Header.Get("X-Notify-Token")
	if token == "" {
		token = r.URL.Query().Get("token")
	}
	if token == "" {
		d.writeJSON(w, http.StatusUnauthorized, APIResponse{Code: 401, Message: "缺少 token"})
		return
	}

	if subtle.ConstantTimeCompare([]byte(token), []byte(expectedToken)) != 1 {
		d.writeJSON(w, http.StatusUnauthorized, APIResponse{Code: 401, Message: "token 无效"})
		return
	}

	if d.libraryScanner == nil {
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 500, Message: "全库扫描器未初始化"})
		return
	}

	delaySeconds := d.config.GetWebhookNotifyDelaySeconds()
	d.logger.Info("📚 [Webhook] 收到通知，%d 秒后触发全库扫描", delaySeconds)

	go func() {
		time.Sleep(time.Duration(delaySeconds) * time.Second)
		if err := d.libraryScanner.TriggerScan(); err != nil {
			d.logger.Warn("📚 [Webhook] 延迟触发失败: %v", err)
		} else {
			d.logger.Info("📚 [Webhook] 已触发全库扫描")
		}
	}()

	d.writeJSON(w, http.StatusOK, APIResponse{
		Code:    200,
		Message: fmt.Sprintf("已接受，%d 秒后触发扫描", delaySeconds),
	})
}
