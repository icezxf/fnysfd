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
	config        *config.Config
	cache         *cache.Cache
	logger        *logger.Logger
	streamHandler *handler.StreamHandler
	startTime     time.Time
	sessions      map[string]session
	sessionMutex  sync.RWMutex
	server        *http.Server
	version       string // 由 main 包注入的版本号
	stopChan      chan struct{}
	stopOnce      sync.Once
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
// version 由 main 包通过 -ldflags "-X main.version=xxx" 注入并传入
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

// writeJSON 统一的 JSON 响应辅助方法，记录编码错误日志
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

// Start 启动面板服务（独立端口）
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

	// 后台定期清理过期 session（每5分钟检查一次，超过24小时的删除）
	go d.cleanupSessionsLoop()

	return nil
}

// cleanupSessionsLoop 定期清理过期 session（支持优雅停止）
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

// cleanupExpiredSessions 清理超过24小时的过期 session
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
	// 关闭 stopChan 通知后台协程退出（仅关闭一次，避免 panic）
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

// authMiddleware 认证中间件（支持API JSON响应和页面重定向）
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

		// 滑动过期：每次合法请求都刷新 session 的 createdAt，延长有效期
		// 用写锁确保并发安全；仅更新时间戳，开销极小
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
// 策略：要求写操作（POST/PUT/DELETE）携带自定义 header X-Requested-With
// 浏览器同源策略保证跨域请求无法携带自定义 header，从而阻止 CSRF 攻击
// GET 请求不受限（读操作不构成 CSRF 风险）
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

// generateSessionID 生成会话ID
func generateSessionID() string {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		// 极端情况下 rand.Read 失败，用当前时间作为兜底熵源（仅用于会话ID，安全性可接受）
		now := time.Now().UnixNano()
		for i := 0; i < 32; i++ {
			bytes[i] = byte(now >> (i % 8 * 8))
		}
	}
	return hex.EncodeToString(bytes)
}

// handleLogin 处理登录
func (d *Dashboard) handleLogin(w http.ResponseWriter, r *http.Request) {
	// /api/login 仅处理 POST；GET 重定向到登录页（/login）
	if r.Method == "GET" {
		http.Redirect(w, r, "/login", http.StatusFound)
		return
	}

	if r.Method != "POST" {
		d.writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Code: 405, Message: "方法不允许"})
		return
	}

	// 限制请求体大小，防止恶意大 body 攻击
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

	// 用户名与密码均使用常量时间比较，避免时序攻击泄露长度/前缀信息
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
			// Secure 根据请求是否为 TLS 动态设置：HTTPS 下启用，HTTP 下不启用
			// 这样既保证生产环境安全，又允许本地 HTTP 调试
			Secure: r.TLS != nil,
		}
		http.SetCookie(w, cookie)

		d.writeJSON(w, http.StatusOK, APIResponse{Code: 200, Message: "登录成功"})
	} else {
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 401, Message: "用户名或密码错误"})
	}
}

// handleLogout 处理登出（仅接受 POST，防止 CSRF 通过 GET 触发登出）
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
		// 同步 Secure 标志，保持与登录一致
		cookie.Secure = r.TLS != nil
		http.SetCookie(w, cookie)
	}

	d.writeJSON(w, http.StatusOK, APIResponse{Code: 200, Message: "已登出"})
}

// showLoginPage 显示登录页面（不需要认证）
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
// 顺序说明：必须先 SetExternalCacheCounts 写入外部缓存计数，
// 再调用 GetStats() 获取快照，否则响应中拿到的是旧值（写入尚未发生）
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

	// 转为 map 以便合并 streamHandler 的性能统计（preload_success 等）
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

	// 合并 streamHandler 性能统计（预加载成功数、总请求数等）
	if d.streamHandler != nil {
		for k, v := range d.streamHandler.GetStats() {
			data[k] = v
		}
	}

	d.writeJSON(w, http.StatusOK, APIResponse{Code: 200, Data: data})
}

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
// 安全限制：只清空应用自身日志（按文件名格式 YYYYMMDD.log 或 YYYYMMDD_N.log 过滤），
// 不删除任意 .log 文件，避免误删用户其他日志
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
		// 仅清理应用自身日志（YYYYMMDD.log 或 YYYYMMDD_N.log 格式）
		// 跳过其他命名规则的 .log 文件，避免误删用户日志
		if !isAppLogFile(base) {
			skippedCount++
			d.logger.Debug("⏭️ 跳过非应用日志文件: %s", base)
			continue
		}
		if err := os.Remove(file); err != nil {
			d.logger.Warn("⚠️ 删除日志文件失败: %s, Error=%v", file, err)
			continue
		}
		deletedCount++
		d.logger.Info("🗑️ 已删除日志文件: %s", base)
	}

	if skippedCount > 0 {
		d.logger.Info("ℹ️ 跳过 %d 个非应用日志文件", skippedCount)
	}
	d.logger.Info("🗑️ 通过管理面板清空日志，共删除 %d 个文件", deletedCount)

	d.writeJSON(w, http.StatusOK, APIResponse{
		Code:    200,
		Message: fmt.Sprintf("已清除 %d 个日志文件", deletedCount),
		Data:    map[string]int{"deleted_count": deletedCount, "skipped_count": skippedCount},
	})
}

// isAppLogFile 判断文件名是否为应用自身日志
// 支持格式：YYYYMMDD.log（当天）与 YYYYMMDD_N.log（轮转切片）
func isAppLogFile(name string) bool {
	// 去除 .log 后缀
	stem := strings.TrimSuffix(name, ".log")
	if stem == name {
		return false // 无 .log 后缀
	}
	// YYYYMMDD：8 位纯数字
	if len(stem) == 8 {
		return isAllDigits(stem)
	}
	// YYYYMMDD_N：8 位数字 + 下划线 + 数字
	if idx := strings.IndexByte(stem, '_'); idx == 8 && len(stem) > 9 {
		return isAllDigits(stem[:8]) && isAllDigits(stem[9:])
	}
	return false
}

// isAllDigits 判断字符串是否全部为数字字符
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

	// 限制请求体大小为 16KB，防止恶意大 body 攻击
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

	// 使用具名 map 变量持有，避免直接对 response.Data 做类型断言（不安全且可读性差）
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
// 支持 limit 查询参数（默认200，最大2000），仅返回最后 limit 行
// 文件大小超过 1MB 时只读取最后 1MB，避免大文件全量加载导致内存峰值
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

	// 若今日日志不存在，则回退到目录中最新的 .log 文件
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
// 若文件大于 1MB，仅读取最后 1MB 以控制内存使用
func readLastLines(path string, limit int) ([]string, error) {
	const maxTailBytes int64 = 1 * 1024 * 1024 // 1MB

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	// 文件大于 1MB 时定位到末尾 1MB 处，跳过开头（旧日志）
	if info.Size() > maxTailBytes {
		if _, seekErr := file.Seek(-maxTailBytes, io.SeekEnd); seekErr != nil {
			return nil, seekErr
		}
		// 定位后首行可能被截断，丢弃到第一个换行符
		reader := bufio.NewReader(file)
		if _, err := reader.ReadBytes('\n'); err != nil && err != io.EOF {
			return nil, err
		}
		// 用双端队列保留最后 limit 行
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

	// 小文件直接全量扫描
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

	d.logger.Info("📋 获取STRM路径列表，共 %d 个Volume映射", len(volumes))

	// 仅保留前端实际使用的字段：
	//   strm_volumes - 字符串列表（向后兼容）
	//   volumes      - 结构化详情（前端渲染使用，避免 split 解析）
	// 已移除冗余/空字段：container_mounts（重复 strm_volumes）、
	//   sync_status（写死空结构）、container_info（永远为空）
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

	// 限制请求体大小，防止恶意大 body
	r.Body = http.MaxBytesReader(w, r.Body, 4*1024)

	var req struct {
		HostPath      string `json:"host_path"`
		ContainerPath string `json:"container_path"`
	}

	bodyBytes, _ := io.ReadAll(r.Body)
	d.logger.Debug("📥 收到添加路径请求: %s", string(bodyBytes))

	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		d.logger.Error("❌ JSON解析失败: %v, 原始数据: %s", err, string(bodyBytes))
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 400, Message: fmt.Sprintf("JSON格式错误: %v，请检查输入内容", err)})
		return
	}

	// 路径遍历安全校验
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
	d.logger.Info("✅ 成功添加STRM Volume: %s (立即可用=%v)", volumeFormat, availableNow)

	composeUpdated := false
	if err := d.config.UpdateComposeFile(); err != nil {
		d.logger.Warn("⚠️  Compose文件更新失败（不影响功能）: %v", err)
	} else {
		composeUpdated = true
		d.logger.Info("📝 Docker Compose文件已自动更新")
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

// validatePath 校验路径安全性，防止路径遍历攻击
// 规则：必须以 "/" 开头；规范化后不允许出现 ".."；不允许为系统关键路径
func validatePath(path, label string) error {
	p := strings.TrimSpace(path)
	if p == "" {
		return fmt.Errorf("%s不能为空", label)
	}
	if !strings.HasPrefix(p, "/") {
		return fmt.Errorf("%s必须以 / 开头", label)
	}
	// 先用 filepath.Clean 规范化路径，再检测 ".."，
	// 避免简单 Contains 误判（如 /a/..b 这类合法路径名会被误拦）
	cleaned := filepath.Clean(p)
	// 规范化后逐段检查是否包含 ".."
	for _, seg := range strings.Split(cleaned, string(filepath.Separator)) {
		if seg == ".." {
			return fmt.Errorf("%s不允许包含 '..' 路径段", label)
		}
	}
	// 系统关键路径黑名单（精确匹配或以此为前缀）
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

	// 限制请求体大小，防止恶意大 body
	r.Body = http.MaxBytesReader(w, r.Body, 4*1024)

	var req struct {
		VolumeFormat string `json:"volume_format"`
	}

	bodyBytes, _ := io.ReadAll(r.Body)
	d.logger.Debug("📥 收到删除路径请求: %s", string(bodyBytes))

	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		d.logger.Error("❌ JSON解析失败: %v, 原始数据: %s", err, string(bodyBytes))
		d.writeJSON(w, http.StatusOK, APIResponse{Code: 400, Message: fmt.Sprintf("JSON格式错误: %v，请检查输入内容", err)})
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

	d.logger.Info("📋 生成Docker Volumes配置，共 %d 个挂载点", len(volumes))

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
	yaml.WriteString("# ============================================\n")
	yaml.WriteString("# ⚠️ 以下配置已包含完整的主机路径和容器路径映射\n")
	yaml.WriteString("# ✅ 可直接复制到 docker-compose.yml 的 volumes 部分\n\n")
	yaml.WriteString("volumes:\n")

	for i, vol := range volumes {
		yaml.WriteString(fmt.Sprintf("  # %d. %s\n", i+1, vol["description"]))
		yaml.WriteString(fmt.Sprintf("  # 主机路径: %s | 容器路径: %s | 权限: %s\n", vol["host_path"], vol["container_path"], vol["permission"]))
		yaml.WriteString(fmt.Sprintf("  - %s\n", vol["volume_format"]))
	}

	yaml.WriteString("\n# 📝 使用说明：\n")
	yaml.WriteString("# 1. 复制以上volumes配置到docker-compose.yml\n")
	yaml.WriteString("# 2. 确保主机路径（冒号左侧）在NAS上存在\n")
	yaml.WriteString("# 3. 点击面板'重启容器'按钮，或执行: docker compose restart\n")
	yaml.WriteString("# 4. 重启后在容器详情的'存储'中查看挂载情况\n")

	return yaml.String()
}

// handleDockerRestart 一键重启容器（应用新的Volume配置）
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
