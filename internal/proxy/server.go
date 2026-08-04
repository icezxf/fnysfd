package proxy

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"fnysfd/internal/cache"
	"fnysfd/internal/config"
	"fnysfd/internal/handler"
	"fnysfd/internal/logger"
	"fnysfd/internal/util"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Server 代理服务器
type Server struct {
	config          *config.Config
	logger          *logger.Logger
	cache           *cache.Cache
	playbackHandler *handler.PlaybackHandler
	streamHandler   *handler.StreamHandler
	proxy           *httputil.ReverseProxy
	proxyMu         sync.RWMutex // 保护 proxy 的并发读写
	currentTarget   string       // 当前目标地址（用于Reload对比）
	startTime       time.Time    // 启动时间（用于uptime统计）
	httpServer      *http.Server
	stopOnce        sync.Once        // 保证 Stop() 幂等，避免重复 close 通道导致 panic
	retryClient     *http.Client     // 主动重试 PlaybackInfo 用的 HTTP 客户端
	targetURL       *url.URL         // 保存目标URL，用于构造主动请求的完整URL
	prefetchRecent  map[string]int64 // 详情页预请求去重（itemID -> unix时间戳）
	prefetchMu      sync.Mutex       // 保护 prefetchRecent
	version         string           // 版本号（由 main 包注入）
}

// NewServer 创建代理服务器
func NewServer(cfg *config.Config, version string) (*Server, error) {
	if version == "" {
		version = "dev"
	}
	log := logger.New(cfg.GetLogLevel(), cfg.GetLogDir())

	targetURL, err := url.Parse(cfg.GetTargetAddr())
	if err != nil {
		return nil, err
	}

	c := cache.NewWithStreamTTL(cfg.GetCacheTTL())

	ph := handler.NewPlaybackHandler(c, log)
	sh := handler.NewStreamHandler(c, log)

	// 🔗 核心优化：绑定PlaybackHandler和StreamHandler，启用智能预加载
	ph.SetStreamHandler(sh)

	proxy := httputil.NewSingleHostReverseProxy(targetURL)

	// 主动重试客户端（用于 PlaybackInfo 400 时主动获取 200）
	// 延长超时时间（飞牛有时需要 5+ 秒才响应）
	retryTransport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          50,
		MaxIdleConnsPerHost:   10,
		MaxConnsPerHost:       20, // 限制对单一目标 host 的最大并发连接
		IdleConnTimeout:       60 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 500 * time.Millisecond,
		ResponseHeaderTimeout: 10 * time.Second,
		DisableKeepAlives:     false,
		ForceAttemptHTTP2:     true,
	}
	retryClient := &http.Client{
		Timeout:   20 * time.Second, // 10s → 20s
		Transport: retryTransport,
	}

	s := &Server{
		config:          cfg,
		logger:          log,
		cache:           c,
		playbackHandler: ph,
		streamHandler:   sh,
		proxy:           proxy,
		currentTarget:   cfg.GetTargetAddr(),
		retryClient:     retryClient,
		targetURL:       targetURL,
		prefetchRecent:  make(map[string]int64),
		version:         version,
	}

	// 将“下一集预取”下沉到 PlaybackHandler 的 STRM 缓存成功回调。
	// 这样无论 PlaybackInfo 来自正常响应、主动预请求还是 400 后重试，只要成功缓存 STRM，
	// 都会走同一条下一集预取链路，避免只看到“📦 [缓存]”但没有预取触发日志。
	ph.SetPlaybackCachedHandler(func(resp *http.Response, itemID string) {
		if resp == nil || resp.Request == nil {
			log.Debug("⏭️ [下一集预取] 回调跳过：缺少原始请求 ItemId=%s", itemID)
			return
		}
		if resp.Request.Header.Get("X-Fnysfd-Next-Prefetch") == "1" {
			log.Debug("⏭️ [下一集预取] 回调跳过：当前请求来自下一集预取 ItemId=%s", itemID)
			return
		}
		if itemID == "" {
			log.Debug("⏭️ [下一集预取] 回调跳过：ItemId为空")
			return
		}
		log.Debug("⏭️ [下一集预取] STRM缓存回调触发: 当前=%s", itemID)
		go s.prefetchNextEpisodesBestEffort(resp.Request, itemID, resp.Request.URL.Query().Get("UserId"))
	})
	log.Info("⏭️ [下一集预取] STRM缓存回调已绑定，详情页成功后将显式触发下一集预取")

	// 新剧无媒体信息兜底
	// 流请求先到但 MediaSource 尚未缓存时，同步轮询 PlaybackInfo，避免原始流请求直转导致3003。
	sh.SetMediaSourceMissHandler(s.prefetchForMediaSourceMiss)

	return s, nil
}

// Start 启动服务器
func (s *Server) Start() error {
	s.startTime = time.Now()

	targetURL, err := url.Parse(s.config.GetTargetAddr())
	if err != nil {
		return err
	}
	s.setupProxy(targetURL)

	mux := http.NewServeMux()
	mux.Handle("/", s.errorHandler(s.loggingMiddleware(http.HandlerFunc(s.handleProxy))))

	// 性能监控端点
	mux.HandleFunc("/stats", s.statsHandler)
	// 健康检查端点
	mux.HandleFunc("/health", s.healthHandler)

	s.httpServer = &http.Server{
		Addr:              s.config.GetListenAddr(),
		Handler:           mux,
		ReadTimeout:       30 * time.Second,
		ReadHeaderTimeout: 10 * time.Second, // 防 Slowloris 攻击
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       180 * time.Second,
		MaxHeaderBytes:    1 << 20, // 1MB，防止超大头部攻击
	}

	s.logger.Info("🌐 反代服务启动 (v%s): http://localhost%s", s.version, s.config.GetListenAddr())
	s.logger.Info("⏭️ [下一集预取] 当前运行版本包含：详情页成功显式触发 + STRM缓存回调触发")
	s.logger.Info("📊 性能监控: http://localhost%s/stats", s.config.GetListenAddr())

	return s.httpServer.ListenAndServe()
}

// setupProxy 配置反向代理的Director和ModifyResponse
func (s *Server) setupProxy(targetURL *url.URL) {
	originalDirector := s.proxy.Director
	s.proxy.Director = func(req *http.Request) {
		originalDirector(req)
		// 仅设置Host头（不含scheme），避免将完整URL写入Host
		req.Host = targetURL.Host

		// 进入详情页就主动预请求 PlaybackInfo
		// 进入详情页（GET /Items/{id}）时立即构造 PlaybackInfo 请求并行发送
		// 用户浏览详情页的几秒钟内，反代已经完成解析，点击播放直接命中缓存
		if itemID, userID, ok := s.isItemDetailRequest(req); ok {
			go s.prefetchForDetailPage(req, itemID, userID)
			return
		}

		// 兜底：点击播放时（PlaybackInfo 请求）的主动预请求
		// 如果用户快速点击播放（详情页预请求还没完成），这里兜底再触发一次
		if s.isPlaybackInfoRequest(req) && req.Method == "GET" {
			go s.proactivePlaybackInfo(req)
		}
	}
	s.proxy.ModifyResponse = s.handleResponse
	// 抑制客户端断开（context canceled）导致的错误日志噪音
	// httputil.ReverseProxy 默认会用 log.Println 输出所有 transport 错误，
	// 但客户端切集/退出导致的 context canceled 是正常现象，不应刷屏
	s.proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
		// 客户端主动取消：降级为 Debug（不计入异常统计）
		if errors.Is(err, context.Canceled) {
			s.logger.Debug("🔌 [客户端断开] %s %s: %v", req.Method, req.URL.Path, err)
			http.Error(rw, "Client closed request", 499) // 499 = Client Closed Request (nginx 约定)
			return
		}
		// 网络中断/broken pipe：同样是客户端问题，降级
		errStr := err.Error()
		if strings.Contains(errStr, "broken pipe") ||
			strings.Contains(errStr, "connection reset") ||
			strings.Contains(errStr, "EOF") {
			s.logger.Debug("🔌 [网络中断] %s %s: %v", req.Method, req.URL.Path, err)
			http.Error(rw, "Connection interrupted", 499)
			return
		}
		// 其他错误（飞牛不可达、超时等）：保留 Warn/Error
		s.logger.Warn("⚠️ [代理错误] %s %s: %v", req.Method, req.URL.Path, err)
		http.Error(rw, "Bad Gateway", http.StatusBadGateway)
	}
}

// handleProxy 加锁读取代理实例并转发请求（保证Reload并发安全）
func (s *Server) handleProxy(w http.ResponseWriter, r *http.Request) {
	s.proxyMu.RLock()
	proxy := s.proxy
	s.proxyMu.RUnlock()
	proxy.ServeHTTP(w, r)
}

// Stop 停止服务器（幂等：可安全多次调用）
func (s *Server) Stop() error {
	var err error
	s.stopOnce.Do(func() {
		s.logger.Info("🛑 正在关闭服务...")

		// 1. 先停止预加载引擎，避免产生新日志
		if s.streamHandler != nil {
			s.streamHandler.Stop()
		}

		// 2. 关闭HTTP服务器，等待在途请求完成（此期间日志仍可用）
		if s.httpServer != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err = s.httpServer.Shutdown(ctx)
			cancel()
		}

		// 3. 停止缓存清理协程（HTTP已关闭，不再有缓存访问）
		if s.cache != nil {
			s.cache.Stop()
		}

		// 4. 最后关闭日志系统，保证上述关闭过程的日志都能落盘
		s.logger.Info("🔚 正在关闭日志系统...")
		s.logger.Close()
	})
	return err
}

// GetCache 获取缓存实例
func (s *Server) GetCache() *cache.Cache {
	return s.cache
}

// GetLogger 获取日志实例
func (s *Server) GetLogger() *logger.Logger {
	return s.logger
}

// GetStreamHandler 获取流处理器
func (s *Server) GetStreamHandler() *handler.StreamHandler {
	return s.streamHandler
}

// Reload 重新加载配置（完整热更新）
func (s *Server) Reload() {
	newTarget := s.config.GetTargetAddr()

	s.logger.SetLevel(s.config.GetLogLevel())
	s.logger.Info("🔄 配置已重载")
	s.logger.Info("   新日志级别: %s", s.config.GetLogLevel())

	// 对比当前已生效的目标地址，检测是否变更（读取加锁，与 handleProxy/Reload 写入互斥）
	s.proxyMu.RLock()
	oldTarget := s.currentTarget
	s.proxyMu.RUnlock()
	if oldTarget != newTarget {
		s.logger.Info("🎯 检测到目标地址变化: %s -> %s", oldTarget, newTarget)

		targetURL, err := url.Parse(newTarget)
		if err != nil {
			s.logger.Error("❌ 解析新目标地址失败: %v", err)
			return
		}

		newProxy := httputil.NewSingleHostReverseProxy(targetURL)
		originalDirector := newProxy.Director
		newProxy.Director = func(req *http.Request) {
			originalDirector(req)
			req.Host = targetURL.Host

			// Reload 后的新 proxy 也要拦截详情页请求和 PlaybackInfo 请求
			if itemID, userID, ok := s.isItemDetailRequest(req); ok {
				go s.prefetchForDetailPage(req, itemID, userID)
				return
			}
			if s.isPlaybackInfoRequest(req) && req.Method == "GET" {
				go s.proactivePlaybackInfo(req)
			}
		}
		newProxy.ModifyResponse = s.handleResponse

		// 加锁替换代理实例，保证并发安全
		s.proxyMu.Lock()
		s.proxy = newProxy
		s.currentTarget = newTarget
		s.targetURL = targetURL // 同步更新 targetURL
		s.proxyMu.Unlock()

		s.logger.Info("✅ 代理服务器已重建，新目标地址生效")
	}

	s.cache.UpdateTTL(s.config.GetCacheTTL())
	s.logger.Info("⏱️ 缓存TTL已更新: %v", s.config.GetCacheTTL())

	s.cache.UpdateMaxItems(s.config.GetMaxCacheItems())
	s.logger.Info("📊 缓存条目数限制已更新: %d", s.config.GetMaxCacheItems())
}

// handleResponse 处理响应
func (s *Server) handleResponse(resp *http.Response) error {
	if !s.isPlaybackInfoRequest(resp.Request) {
		return nil
	}

	// 飞牛服务器第一次 PlaybackInfo 常返回 400，第二次才返回 200
	// 突破：400 时主动重试，提前缓存 MediaSource + 触发预加载
	if resp.StatusCode != http.StatusOK {
		// 日志精简，不打 Method 和完整 Path（Path 已在重试日志中输出）
		s.logger.Info("⚠️ [PlaybackInfo] %d 响应，启动重试: %s",
			resp.StatusCode, resp.Request.URL.Path)
		go s.retryPlaybackInfo(resp.Request)
		return nil
	}

	// 成功响应只打 Debug，减少日志噪音（关键信息在 playbackHandler.Handle 中输出）
	s.logger.Debug("📥 [PlaybackInfo] 200 响应: %s", resp.Request.URL.Path)

	// 限制读取大小为10MB，防止恶意大响应体导致OOM
	body, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
	if err != nil {
		s.logger.Error("❌ [响应处理] 读取响应体失败: %v", err)
		return err
	}
	resp.Body.Close()

	// 空响应体跳过处理
	if len(body) == 0 {
		s.logger.Debug("📊 [响应处理] 响应体为空，跳过处理")
		return nil
	}

	s.logger.Debug("📊 [响应处理] 响应体大小: %d bytes", len(body))

	newBody, err := s.playbackHandler.Handle(resp, body)
	if err != nil {
		s.logger.Error("❌ [响应处理] PlaybackInfo处理失败: %v", err)
	}

	// 下一集预取已由 PlaybackHandler.Handle 内部的 STRM 缓存回调统一触发，
	// 无需在此重复调用（handleResponse → Handle → onPlaybackCached → prefetchNextEpisodesBestEffort）

	// 更新 Content-Length 以匹配新响应体大小，并移除 Transfer-Encoding 避免冲突
	resp.ContentLength = int64(len(newBody))
	resp.Header.Set("Content-Length", strconv.Itoa(len(newBody)))
	resp.Header.Del("Transfer-Encoding")
	resp.Body = io.NopCloser(bytes.NewBuffer(newBody))

	s.logger.Debug("✅ [响应处理] PlaybackInfo处理完成")
	return nil
}

// retryPlaybackInfo 主动重试 PlaybackInfo 请求
// 飞牛服务器第一次返回 400/403 时，反代主动向飞牛重发请求，拿到 200 后提前缓存 MediaSource
// 核心优化（配合 prefetchForDetailPage 持续重试）：
//   - 最多 5 次重试，前段 500ms 高频探测，抢冷启动
//   - 这是用户点击播放时的兜底：如果详情页预请求还没拿到 200，这里继续轮询
//   - 飞牛 probe 期间返回 400，probe 完成后立即 200 → PreloadStrmSync 同步缓存
// 防止无限重试的机制：
//   - 最多 5 次（配合详情页预取，总轮询窗口足够覆盖常见 probe 时间）
//   - 客户端不会无限发 PlaybackInfo（用户退出播放界面后就停止）
func (s *Server) retryPlaybackInfo(originalReq *http.Request) {
	// 完全不做去重，400/403 响应每次都必须能触发重试
	// 保存请求信息（originalReq 可能在响应处理后被回收）
	s.proxyMu.RLock()
	targetURL := s.targetURL
	s.proxyMu.RUnlock()

	reqURL := targetURL.Scheme + "://" + targetURL.Host + originalReq.URL.Path
	if originalReq.URL.RawQuery != "" {
		reqURL += "?" + originalReq.URL.RawQuery
	}
	reqMethod := originalReq.Method
	reqPath := originalReq.URL.Path
	reqHost := targetURL.Host
	reqHeaders := originalReq.Header.Clone()
	reqHeaders.Del("Accept-Encoding")    // 让 Transport 自动处理 gzip
	reqHeaders.Set("Accept", "application/json") // 强制 JSON 响应，避免飞牛返回 HTML

	// 冷启动优化，缩短首次兜底重试延迟
	time.Sleep(100 * time.Millisecond)

	s.logger.Info("🔄 [重试] 开始: %s", reqPath)

	// 冷启动兜底重试从3次提升到5次，前段高频探测probe完成点
	const maxAttempts = 5
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(context.Background(), reqMethod, reqURL, nil)
		if err != nil {
			s.logger.Warn("❌ [重试] 创建请求失败: %v", err)
			return
		}
		req.Header = reqHeaders.Clone()
		req.Host = reqHost

		resp, err := s.retryClient.Do(req)
		if err != nil {
			s.logger.Debug("⚠️ [重试] 第%d次请求失败: %v", attempt, err)
			if attempt < maxAttempts {
				time.Sleep(playbackProbeRetryInterval(attempt))
			}
			continue
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			s.logger.Debug("⚠️ [重试] 第%d次状态码: %d (飞牛probe中)", attempt, resp.StatusCode)
			if attempt < maxAttempts {
				time.Sleep(playbackProbeRetryInterval(attempt))
			}
			continue
		}

		// 200！
		body, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
		resp.Body.Close()
		if err != nil {
			s.logger.Warn("⚠️ [重试] 读取响应体失败: %v", err)
			return
		}

		if len(body) == 0 {
			s.logger.Warn("⚠️ [重试] 响应体为空")
			return
		}

		if attempt == 1 {
			s.logger.Info("✅ [重试] 首次成功: %s", reqPath)
		} else {
			s.logger.Info("✅ [重试] 第%d次成功: %s (飞牛probe完成)", attempt, reqPath)
		}
		_, _ = s.playbackHandler.Handle(resp, body)
		// 下一集预取由 Handle 内部的 STRM 缓存回调统一触发，无需重复调用
		return
	}

	s.logger.Debug("⚠️ [重试] %d次尝试均未成功(飞牛probe超时): %s", maxAttempts, reqPath)
}

// proactivePlaybackInfo 主动预请求
// 在请求转发给飞牛的同时，立即并行发送主动预请求
// 核心优化：单次尝试，不重试
//   - 飞牛返回 400 是因为它在后台准备，重试无用且占资源
//   - 1 次请求足以触发飞牛准备，等客户端下次发 PlaybackInfo 时会拿到 200
// 工作流程：
//
//	客户端发 PlaybackInfo 请求 → Director 拦截 → 转发原始请求 + 启动主动预请求
//	→ 100ms 后主动预请求发出（和原始请求并行）
//	→ 200：缓存 MediaSource + 触发预加载
//	→ 非 200：不重试，等客户端下次请求时飞牛已准备好
func (s *Server) proactivePlaybackInfo(originalReq *http.Request) {
	// 智能去重 - 检查 MediaSource 是否已缓存
	// - 已缓存：直接跳过（不需要预请求，避免占用资源）
	// - 未缓存：跳过去重，强制解析（详情页预请求可能失败了，需要重新解析）
	itemID := extractItemIDFromPath(originalReq.URL.Path)
	mediaSourceID := originalReq.URL.Query().Get("MediaSourceId")

	// 先检查缓存：如果 MediaSource 已缓存且直链已解析，则跳过
	if itemID != "" {
		if source, found := s.cache.GetByItemID(itemID); found {
			if _, urlFound := s.cache.GetStreamURL(source.ID); urlFound {
				s.logger.Debug("⏭️ [主动预请求] 跳过（已缓存且直链已解析）: ItemId=%s", itemID)
				return
			}
		}
	}
	// 未缓存时跳过去重，强制解析（3 秒去重只防止极端重复触发）
	if s.shouldSkipPrefetch(buildPrefetchKey("proactive", itemID, mediaSourceID), 3, "主动预请求") {
		return
	}

	// 保存请求信息（originalReq 可能在响应处理后被回收）
	s.proxyMu.RLock()
	targetURL := s.targetURL
	s.proxyMu.RUnlock()

	reqURL := targetURL.Scheme + "://" + targetURL.Host + originalReq.URL.Path
	if originalReq.URL.RawQuery != "" {
		reqURL += "?" + originalReq.URL.RawQuery
	}
	reqMethod := originalReq.Method
	reqPath := originalReq.URL.Path
	reqHost := targetURL.Host
	reqHeaders := originalReq.Header.Clone()
	reqHeaders.Del("Accept-Encoding")    // 让 Transport 自动处理 gzip
	reqHeaders.Set("Accept", "application/json") // 强制 JSON 响应，避免飞牛返回 HTML

	// 冷启动优化，缩短主动预请求延迟
	time.Sleep(100 * time.Millisecond)

	s.logger.Info("🚀 [主动] 开始: %s", reqPath)

	// 单次尝试，不重试
	req, err := http.NewRequestWithContext(context.Background(), reqMethod, reqURL, nil)
	if err != nil {
		s.logger.Warn("❌ [主动] 创建请求失败: %v", err)
		return
	}

	req.Header = reqHeaders.Clone()
	req.Host = reqHost

	resp, err := s.retryClient.Do(req)
	if err != nil {
		// 失败用 Debug，减少噪音
		s.logger.Debug("⚠️ [主动] 请求失败: %v", err)
		return
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		s.logger.Debug("⚠️ [主动] 状态码: %d (飞牛准备中)", resp.StatusCode)
		return
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
	resp.Body.Close()
	if err != nil {
		s.logger.Warn("⚠️ [主动] 读取响应体失败: %v", err)
		return
	}

	if len(body) == 0 {
		s.logger.Warn("⚠️ [主动] 响应体为空")
		return
	}

	// 成功只打简短日志
	s.logger.Info("✅ [主动] 成功: %s", reqPath)
	_, _ = s.playbackHandler.Handle(resp, body)
	// 下一集预取由 Handle 内部的 STRM 缓存回调统一触发，无需重复调用
}

// isItemDetailRequest 检查是否是详情页请求
// 匹配 GET /Items/{id} 或 GET /Users/{userId}/Items/{id}（路径以 /Items/{GUID} 结尾）
// 排除 /Items/{id}/Images、/Items/{id}/PlaybackInfo、/Items/{id}/AdditionalParts 等子路径
// 返回 (itemID, userID, ok)，userID 可能为空（路径不含 /Users/{uid}/ 时）
func (s *Server) isItemDetailRequest(req *http.Request) (string, string, bool) {
	if req.Method != "GET" {
		return "", "", false
	}

	path := req.URL.Path
	// 移除 /emby 或 /Emby 前缀（统一处理）
	pathLower := strings.ToLower(path)
	pathLower = strings.TrimPrefix(pathLower, "/emby")
	pathLower = strings.TrimPrefix(pathLower, "/")

	parts := strings.Split(pathLower, "/")
	if len(parts) < 2 {
		return "", "", false
	}

	// 最后一段是 ItemID
	itemID := parts[len(parts)-1]
	// ItemID 长度 >= 16 且像 GUID（飞牛的 ItemID 是 32 位十六进制）
	if len(itemID) < 16 || !util.IsGUIDLikeLoose(itemID) {
		return "", "", false
	}

	// 倒数第二段必须是 "items"
	itemsIdx := len(parts) - 2
	if parts[itemsIdx] != "items" {
		return "", "", false
	}

	// 提取 UserId（路径形如 /users/{uid}/items/{id}）
	userID := ""
	if itemsIdx >= 2 && parts[itemsIdx-2] == "users" {
		uidCandidate := parts[itemsIdx-1]
		if len(uidCandidate) >= 16 && util.IsGUIDLikeLoose(uidCandidate) {
			userID = uidCandidate
		}
	}

	// 此时路径形如 .../items/{id}，正好是详情页请求
	// 子路径请求（/Items/{id}/Images、/Items/{id}/PlaybackInfo 等）因为后面还有路径段，
	// 不会被误判（它们的最后一段不是 GUID，而是 Images/PlaybackInfo 等关键字）
	s.logger.Debug("🎬 [详情页检测] 命中: %s %s (ItemId=%s, UserId=%s)", req.Method, path, itemID, userID)
	return itemID, userID, true
}

// prefetchForDetailPage 进入详情页时主动预请求 PlaybackInfo
// 用户进入详情页 → 客户端发 GET /Items/{id} → Director 拦截 → 主动构造 PlaybackInfo 请求
// 核心优化（解决电影 probe 慢导致首播延迟）：
//   - 持续重试（前段 500ms 高频轮询，后段降频），直到飞牛 probe 完成
//   - 电影文件大，飞牛 probe 可能需要 2-8s
//   - 持续轮询，probe 一完成立即拿到 200 → PreloadStrmSync 同步缓存直链
//   - 电视剧 probe 快（多集缓存），通常首次就 200，不会重试
// 工作流程：
//
//	用户进入详情页 → 50ms 后反代开始轮询 PlaybackInfo
//	→ 飞牛 probe 中：返回 400 → 1s 后重试 → 400 → 1s 后重试 → ...
//	→ 飞牛 probe 完成：返回 200 → PreloadStrmSync → 直链就绪
//	→ 用户点击播放 → 流请求直接命中缓存 → 302 立即返回
func (s *Server) prefetchForDetailPage(originalReq *http.Request, itemID string, userID string) {
	// 详情页预请求 30 秒去重（key 不含 mediaSourceId，因为详情页请求不带这个参数）
	// 但来自"下一集预取"的调用（带 X-Fnysfd-Next-Prefetch 头）跳过去重，
	// 因为这是预取链路主动发起的，不应被用户之前浏览详情页的去重记录阻止
	isFromPrefetch := originalReq.Header.Get("X-Fnysfd-Next-Prefetch") == "1"
	if !isFromPrefetch {
		if s.shouldSkipPrefetch(buildPrefetchKey("detail", itemID, ""), 30, "详情页预请求") {
			return
		}
	} else {
		// 预取链路跳过去重检查，但仍需设置去重标记，
		// 防止用户随后浏览该集详情页时重复触发预取（导致成功日志重复打印）
		s.markPrefetchDone(buildPrefetchKey("detail", itemID, ""))
		s.logger.Debug("🎬 [详情页] 预取链路调用，跳过检查但设置去重标记: %s", itemID)
	}

	s.proxyMu.RLock()
	targetURL := s.targetURL
	s.proxyMu.RUnlock()

	// 构造 PlaybackInfo 请求 URL（飞牛/Emby 标准路径：/emby/Items/{id}/PlaybackInfo）
	playbackPath := "/emby/Items/" + itemID + "/PlaybackInfo"

	// 关键修复：合并 query 参数，确保 UserId 被带上
	query := originalReq.URL.Query()
	if userID != "" && query.Get("UserId") == "" {
		query.Set("UserId", userID)
	}

	fullURL := targetURL.Scheme + "://" + targetURL.Host + playbackPath
	if encoded := query.Encode(); encoded != "" {
		fullURL += "?" + encoded
	}

	reqHeaders := originalReq.Header.Clone()
	reqHeaders.Del("Accept-Encoding")    // 让 Transport 自动处理 gzip，避免压缩响应导致解析失败
	reqHeaders.Set("Accept", "application/json") // 强制 JSON 响应，避免飞牛返回 HTML
	reqHost := targetURL.Host

	// 冷启动优化，详情页预取更早启动
	time.Sleep(50 * time.Millisecond)

	s.logger.Info("🎬 [详情页] 开始: %s", itemID)

	// 冷启动优化，前段高频轮询，后段降频
	// 前10次每500ms探测，之后1s，再之后2s；既抢冷启动，又避免长期刷屏
	const maxAttempts = 30
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// 每次重新构造请求（http.Request 发送后 Body 等字段可能被修改）
		req, err := http.NewRequestWithContext(context.Background(), "GET", fullURL, nil)
		if err != nil {
			s.logger.Warn("❌ [详情页] 创建请求失败: %v", err)
			return
		}
		req.Header = reqHeaders.Clone()
		req.Host = reqHost

		resp, err := s.retryClient.Do(req)
		if err != nil {
			s.logger.Debug("⚠️ [详情页] 第%d次请求失败: %v", attempt, err)
			if attempt < maxAttempts {
				time.Sleep(playbackProbeRetryInterval(attempt))
			}
			continue
		}

		// 非 200：飞牛正在 probe，等 2s 后重试
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			// probe 期间的重试日志统一降为 Debug，避免 Info 级别刷屏
			// 如需排查 probe 慢的问题，把日志级别调成 debug 即可看到每次重试状态
			s.logger.Debug("⏳ [详情页] 第%d次: %d (飞牛probe中, 已等%ds): %s",
				attempt, resp.StatusCode, int(prefetchElapsedSeconds(attempt)), itemID)
			if attempt < maxAttempts {
				time.Sleep(playbackProbeRetryInterval(attempt))
			}
			continue
		}

		// 200！飞牛 probe 完成，读取响应体
		body, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
		resp.Body.Close()
		if err != nil {
			s.logger.Warn("⚠️ [详情页] 读取响应体失败: %v", err)
			return
		}

		if len(body) == 0 {
			s.logger.Warn("⚠️ [详情页] 响应体为空")
			return
		}

		// 成功日志：首次成功 vs 重试成功（重试成功说明飞牛 probe 刚完成）
		if attempt == 1 {
			s.logger.Info("✅ [详情页] 首次成功: %s", itemID)
		} else {
			s.logger.Info("✅ [详情页] 第%d次成功(约等%.1fs): %s (飞牛probe完成)",
				attempt, prefetchElapsedSeconds(attempt), itemID)
		}
		_, _ = s.playbackHandler.Handle(resp, body)
		// 下一集预取由 Handle 内部的 STRM 缓存回调统一触发，无需重复调用
		return
	}

	// 30 次都失败（飞牛 probe 超过约33s，极少见），等用户点击播放时客户端请求兜底
	s.logger.Debug("⚠️ [详情页] %d次尝试均未成功(飞牛probe超时约33s): %s", maxAttempts, itemID)
}

// prefetchForMediaSourceMiss 流请求发现 MediaSource 未缓存时的同步兜底
// 新剧没有媒体信息时，播放器可能先发流请求，而飞牛还在 probe。
// 这里同步轮询 PlaybackInfo，拿到 MediaSource 后让当前流请求继续走 STRM 解析，避免转发未准备流导致3003。
func (s *Server) prefetchForMediaSourceMiss(originalReq *http.Request, itemID string, mediaSourceID string) bool {
	if itemID == "" {
		return false
	}

	// 如果已经有缓存，直接返回
	if mediaSourceID != "" {
		if _, found := s.cache.Get(mediaSourceID); found {
			return true
		}
	}
	if _, found := s.cache.GetByItemID(itemID); found {
		return true
	}

	s.proxyMu.RLock()
	targetURL := s.targetURL
	s.proxyMu.RUnlock()

	playbackPath := "/emby/Items/" + itemID + "/PlaybackInfo"
	query := originalReq.URL.Query()
	fullURL := targetURL.Scheme + "://" + targetURL.Host + playbackPath
	if encoded := query.Encode(); encoded != "" {
		fullURL += "?" + encoded
	}

	reqHeaders := originalReq.Header.Clone()
	reqHeaders.Del("Accept-Encoding")    // 让 Transport 自动处理 gzip
	reqHeaders.Set("Accept", "application/json") // 强制 JSON 响应，避免飞牛返回 HTML
	reqHost := targetURL.Host

	s.logger.Info("🧩 [MediaSource兜底] 轮询PlaybackInfo: ItemId=%s, MS=%s", itemID, mediaSourceID)

	// 总等待约 10 秒：无媒体信息新剧通常需要 probe，等待比直接3003更友好
	const maxAttempts = 18
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(context.Background(), "GET", fullURL, nil)
		if err != nil {
			s.logger.Warn("❌ [MediaSource兜底] 创建请求失败: %v", err)
			return false
		}
		req.Header = reqHeaders.Clone()
		req.Host = reqHost

		resp, err := s.retryClient.Do(req)
		if err != nil {
			s.logger.Debug("⚠️ [MediaSource兜底] 第%d次请求失败: %v", attempt, err)
			if attempt < maxAttempts {
				time.Sleep(mediaSourceMissRetryInterval(attempt))
			}
			continue
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			s.logger.Debug("⏳ [MediaSource兜底] 第%d次: %d (probe中): %s", attempt, resp.StatusCode, itemID)
			if attempt < maxAttempts {
				time.Sleep(mediaSourceMissRetryInterval(attempt))
			}
			continue
		}

		body, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
		resp.Body.Close()
		if err != nil || len(body) == 0 {
			s.logger.Debug("⚠️ [MediaSource兜底] 响应读取失败或为空: %v", err)
			if attempt < maxAttempts {
				time.Sleep(mediaSourceMissRetryInterval(attempt))
			}
			continue
		}

		_, _ = s.playbackHandler.Handle(resp, body)

		// 检查 MediaSource 是否已缓存
		var msFound bool
		var cachedMediaSourceID string
		if mediaSourceID != "" {
			if _, found := s.cache.Get(mediaSourceID); found {
				msFound = true
				cachedMediaSourceID = mediaSourceID
			}
		}
		if !msFound {
			if source, found := s.cache.GetByItemID(itemID); found {
				msFound = true
				cachedMediaSourceID = source.ID
			}
		}

		if msFound {
			// MediaSource 已缓存，等待 STRM URL 解析完成
			// Handle 内部已调用 PreloadStrmSync：
			//   - 直链/passthrough 场景：URL 已立即缓存，WaitForURLResolution 立即返回
			//   - 需 HTTP 解析场景：executePreload 正在异步执行，等待其完成
			// 超时 5s：足够覆盖 fast(2s)+medium(4s) 两档解析，超时后仍返回 true 让流请求自行解析
			s.logger.Info("✅ [MediaSource兜底] 第%d次成功: ItemId=%s, 等待URL解析...", attempt, itemID)
			if s.streamHandler.WaitForURLResolution(cachedMediaSourceID, itemID, 5*time.Second) {
				s.logger.Info("✅ [MediaSource兜底] URL已就绪: ItemId=%s", itemID)
			} else {
				s.logger.Warn("⚠️ [MediaSource兜底] URL解析未完成，流请求将自行解析: ItemId=%s", itemID)
			}
			// 下一集预取由 Handle 内部的 STRM 缓存回调统一触发，无需重复调用
			return true
		}

		if attempt < maxAttempts {
			time.Sleep(mediaSourceMissRetryInterval(attempt))
		}
	}

	s.logger.Warn("⚠️ [MediaSource兜底] 超时仍未准备好: ItemId=%s, MS=%s", itemID, mediaSourceID)
	return false
}

func mediaSourceMissRetryInterval(attempt int) time.Duration {
	if attempt <= 8 {
		return 500 * time.Millisecond
	}
	return 1 * time.Second
}

// prefetchNextEpisodesBestEffort 预取同一季后续集
// 解决新剧没有媒体信息时“当前集刚能播，下一集仍要重新probe导致慢/3003”的问题。
// 这是尽力而为逻辑：如果接口字段不匹配或查询失败，不影响当前播放。
func (s *Server) prefetchNextEpisodesBestEffort(originalReq *http.Request, itemID string, userID string) {
	if itemID == "" {
		s.logger.Debug("⏭️ [下一集预取] 跳过：ItemId为空")
		return
	}
	if !config.Global.GetEnablePreload() {
		s.logger.Debug("⏭️ [下一集预取] 跳过：预加载开关已关闭 当前=%s", itemID)
		return
	}
	if originalReq == nil {
		s.logger.Debug("⏭️ [下一集预取] 跳过：原始请求为空 当前=%s", itemID)
		return
	}
	if originalReq.Header.Get("X-Fnysfd-Next-Prefetch") == "1" {
		s.logger.Debug("⏭️ [下一集预取] 跳过：当前请求来自下一集预取 当前=%s", itemID)
		return
	}
	// 去重检查前置：避免多个触发源同时启动 goroutine 导致 "收到触发" 日志刷屏
	// （STRM缓存回调、handleResponse、prefetchForDetailPage 等多个路径会同时触发）
	if s.shouldSkipPrefetch(buildPrefetchKey("next", itemID, ""), 20, "下一集预取") {
		s.logger.Debug("⏭️ [下一集预取] 跳过：20秒内已触发 当前=%s", itemID)
		return
	}
	s.logger.Info("⏭️ [下一集预取] 收到触发: 当前=%s", itemID)
	if userID == "" {
		userID = originalReq.URL.Query().Get("UserId")
	}

	s.logger.Debug("⏭️ [下一集预取] 准备查询同季后续集: 当前=%s", itemID)

	s.proxyMu.RLock()
	targetURL := s.targetURL
	s.proxyMu.RUnlock()

	reqHeaders := originalReq.Header.Clone()
	// ⚠️ 关键修复：移除 Accept-Encoding 头，让 Go HTTP Transport 自动处理 gzip
	// 原因：克隆的请求头包含 Accept-Encoding: gzip（来自客户端原始请求），
	// 但 Go http.Client 在请求头中显式设置 Accept-Encoding 时不会自动解压响应。
	// 导致飞牛返回 gzip 压缩的 JSON，json.Unmarshal 解析失败，预取静默中断。
	// 移除后 Transport 会自己添加 Accept-Encoding 并自动解压。
	reqHeaders.Del("Accept-Encoding")
	reqHeaders.Set("Accept", "application/json") // 强制 JSON 响应，避免飞牛返回 HTML
	reqHost := targetURL.Host

	// 1. 查询当前集详情，拿 ParentId / IndexNumber
	// ⚠️ 关键修复：使用 /emby/Users/{userId}/Items/{id} 而非 /emby/Items/{id}
	// 原因：飞牛 fnOS 会拦截 /emby/Items/{id} 返回 Web UI HTML 页面（非 JSON），
	// 但 /emby/Users/{userId}/Items/{id} 是纯 API 端点，不会被拦截。
	// /emby/Items/{id}/PlaybackInfo 不受影响，所以详情页预取一直正常。
	var detailURL string
	query := originalReq.URL.Query()
	if userID != "" {
		detailURL = targetURL.Scheme + "://" + targetURL.Host + "/emby/Users/" + userID + "/Items/" + itemID
	} else {
		detailURL = targetURL.Scheme + "://" + targetURL.Host + "/emby/Items/" + itemID
	}
	if len(query) > 0 {
		// 移除 UserId 参数（已嵌入 URL 路径），避免重复
		query.Del("UserId")
		if encoded := query.Encode(); encoded != "" {
			detailURL += "?" + encoded
		}
	}

	req, err := http.NewRequestWithContext(context.Background(), "GET", detailURL, nil)
	if err != nil {
		s.logger.Warn("⏭️ [下一集预取] 创建详情请求失败: %v itemID=%s", err, itemID)
		return
	}
	req.Header = reqHeaders.Clone()
	req.Host = reqHost

	resp, err := s.retryClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		status := 0
		if resp != nil {
			status = resp.StatusCode
			resp.Body.Close()
		}
		s.logger.Warn("⏭️ [下一集预取] 当前集详情查询失败: status=%d err=%v itemID=%s", status, err, itemID)
		return
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	resp.Body.Close()
	if err != nil || len(body) == 0 {
		s.logger.Warn("⏭️ [下一集预取] 当前集详情响应为空或读取失败: %v itemID=%s", err, itemID)
		return
	}

	var detail struct {
		ID          string `json:"Id"`
		Name        string `json:"Name"` // 剧集名称
		ParentID    string `json:"ParentId"`
		SeriesID    string `json:"SeriesId"`
		SeasonID    string `json:"SeasonId"`
		IndexNumber int    `json:"IndexNumber"`
		Type        string `json:"Type"` // Movie/Episode/Series 等
	}
	if err := json.Unmarshal(body, &detail); err != nil {
		preview := string(body)
		if len(preview) > 200 {
			preview = preview[:200]
		}
		s.logger.Warn("⏭️ [下一集预取] 当前集详情JSON解析失败: %v itemID=%s bodyLen=%d bodyPreview=%s", err, itemID, len(body), preview)
		return
	}
	// 电影/其他非剧集类型不需要预取下一集
	if detail.Type != "" && detail.Type != "Episode" {
		s.logger.Info("⏭️ [下一集预取] 跳过：非剧集类型 Type=%s 名称=%s", detail.Type, detail.Name)
		return
	}
	if detail.ParentID == "" && detail.SeasonID != "" {
		detail.ParentID = detail.SeasonID
	}
	if detail.ParentID == "" {
		s.logger.Debug("⏭️ [下一集预取] 当前集缺少 ParentId/SeasonId，无法定位同季: 当前=%s", itemID)
		return
	}
	// 日志加入剧集名称
	curName := detail.Name
	if curName == "" {
		curName = itemID
	}
	s.logger.Info("⏭️ [下一集预取] 当前集: %s (第%d集)，准备查询同季列表", curName, detail.IndexNumber)
	s.logger.Debug("⏭️ [下一集预取] 当前集信息: ParentId=%s SeriesId=%s SeasonId=%s Index=%d",
		detail.ParentID, detail.SeriesID, detail.SeasonID, detail.IndexNumber)

	// 2. 查询同一季列表
	listQuery := url.Values{}
	listQuery.Set("ParentId", detail.ParentID)
	listQuery.Set("IncludeItemTypes", "Episode")
	listQuery.Set("Recursive", "false")
	listQuery.Set("SortBy", "IndexNumber")
	listQuery.Set("SortOrder", "Ascending")
	listQuery.Set("Fields", "BasicSyncInfo,MediaSources")
	listQuery.Set("Limit", "200")
	if userID != "" {
		listQuery.Set("UserId", userID)
	}

	// 同季列表查询 URL 优先级：
	// 1. /emby/Shows/{seriesId}/Episodes - 飞牛不会拦截，最可靠
	// 2. /emby/Users/{userId}/Items - 飞牛不会拦截
	// 3. /emby/Items - 可能被飞牛拦截返回 HTML，作为最后 fallback
	listURLs := []string{}
	if detail.SeriesID != "" {
		episodesQuery := url.Values{}
		if userID != "" {
			episodesQuery.Set("UserId", userID)
		}
		if detail.SeasonID != "" {
			episodesQuery.Set("SeasonId", detail.SeasonID)
		}
		episodesQuery.Set("Fields", "BasicSyncInfo,MediaSources")
		episodesQuery.Set("Limit", "200")
		listURLs = append(listURLs, targetURL.Scheme+"://"+targetURL.Host+"/emby/Shows/"+detail.SeriesID+"/Episodes?"+episodesQuery.Encode())
	}
	if userID != "" {
		listURLs = append(listURLs, targetURL.Scheme+"://"+targetURL.Host+"/emby/Users/"+userID+"/Items?"+listQuery.Encode())
	}
	listURLs = append(listURLs, targetURL.Scheme+"://"+targetURL.Host+"/emby/Items?"+listQuery.Encode())

	var listResp struct {
		Items []struct {
			ID          string `json:"Id"`
			Name        string `json:"Name"` // 剧集名称
			IndexNumber int    `json:"IndexNumber"`
		} `json:"Items"`
	}

	for _, listURL := range listURLs {
		req, err = http.NewRequestWithContext(context.Background(), "GET", listURL, nil)
		if err != nil {
			continue
		}
		req.Header = reqHeaders.Clone()
		req.Host = reqHost

		resp, err = s.retryClient.Do(req)
		if err != nil || resp.StatusCode != http.StatusOK {
			status := 0
			if resp != nil {
				status = resp.StatusCode
				resp.Body.Close()
			}
			s.logger.Debug("⏭️ [下一集预取] 同季列表接口失败: status=%d err=%v url=%s", status, err, listURL)
			continue
		}
		body, err = io.ReadAll(io.LimitReader(resp.Body, 5*1024*1024))
		resp.Body.Close()
		if err != nil || len(body) == 0 {
			s.logger.Warn("⏭️ [下一集预取] 同季列表响应为空或读取失败: %v url=%s", err, listURL)
			continue
		}
		listResp.Items = nil
		if err := json.Unmarshal(body, &listResp); err != nil {
			s.logger.Warn("⏭️ [下一集预取] 同季列表JSON解析失败: %v url=%s bodyLen=%d", err, listURL, len(body))
			// 打印 body 前200字符，帮助诊断是否返回 HTML 错误页
			if len(body) > 0 {
				preview := string(body)
				if len(preview) > 200 {
					preview = preview[:200]
				}
				s.logger.Warn("⏭️ [下一集预取] 同季列表响应预览: %s", preview)
			}
			continue
		}
		if len(listResp.Items) > 0 {
			s.logger.Debug("⏭️ [下一集预取] 同季列表获取成功: %d 集", len(listResp.Items))
			break
		}
		s.logger.Debug("⏭️ [下一集预取] 同季列表为空: url=%s", listURL)
	}

	if len(listResp.Items) == 0 {
		s.logger.Info("⏭️ [下一集预取] 未获取到同季列表，跳过: 当前=%s", curName)
		return
	}

	s.logger.Info("⏭️ [下一集预取] 同季列表获取成功: %d 集", len(listResp.Items))

	// 3. 预取后续2集，避免整季递归预取占资源
	// IndexNumber=0 兼容：飞牛 API 有时不返回 IndexNumber（返回0），
	// 此时用列表中的位置代替集号比较（列表已按 IndexNumber 排序）
	// 先找到当前集在列表中的位置，用于 IndexNumber=0 时的回退比较
	curPos := -1
	for i, ep := range listResp.Items {
		if ep.ID == itemID {
			curPos = i
			break
		}
	}

	// 诊断日志：打印列表中每集的 IndexNumber 和位置，帮助排查跳集问题
	for i, ep := range listResp.Items {
		s.logger.Debug("⏭️ [下一集预取] 列表[%d]: ID=%s Name=%s IndexNumber=%d (当前集Index=%d curPos=%d)",
			i, ep.ID[:12], ep.Name, ep.IndexNumber, detail.IndexNumber, curPos)
	}

	if curPos < 0 {
		s.logger.Warn("⏭️ [下一集预取] 当前集不在列表中，可能列表来源不一致: itemID=%s", itemID)
	}

	prefetched := 0
	alreadyCached := 0
	for i, ep := range listResp.Items {
		if ep.ID == "" || ep.ID == itemID {
			continue
		}
		// 集号比较逻辑：
		// 1. 两者都有有效集号（>0）→ 直接比较集号
		// 2. 当前集有集号但列表项没有 → 用列表位置回退（列表已按 IndexNumber 排序）
		// 3. 两者都没有集号 → 用列表位置回退
		shouldSkip := false
		if detail.IndexNumber > 0 && ep.IndexNumber > 0 {
			// 两者都有有效集号，直接比较
			if ep.IndexNumber <= detail.IndexNumber {
				shouldSkip = true
			}
		} else if curPos >= 0 {
			// 至少一方没有有效集号，用列表位置回退
			// 只预取当前位置之后的集
			if i <= curPos {
				shouldSkip = true
			}
		}
		if shouldSkip {
			continue
		}
		if _, found := s.cache.GetByItemID(ep.ID); found {
			alreadyCached++
			s.logger.Debug("⏭️ [下一集预取] 跳过(已缓存): %s (第%d集)", ep.Name, ep.IndexNumber)
			continue
		}

		nextReq := originalReq.Clone(context.Background())
		nextReq.Header = originalReq.Header.Clone()
		nextReq.Header.Set("X-Fnysfd-Next-Prefetch", "1")
		// 日志加入下一集名称
		epName := ep.Name
		if epName == "" {
			epName = ep.ID
		}
		s.logger.Info("⏭️ [下一集预取] 开始: %s → 下一集: %s (第%d集)", curName, epName, ep.IndexNumber)
		go s.prefetchForDetailPage(nextReq, ep.ID, userID)
		prefetched++
		if prefetched >= 2 {
			break
		}
	}

	// 预取结果摘要（Info级，让用户看到预取了什么、跳过了什么）
	if prefetched > 0 {
		s.logger.Info("⏭️ [下一集预取] 完成: %s | 新预取 %d 集, 已缓存 %d 集", curName, prefetched, alreadyCached)
	} else {
		s.logger.Info("⏭️ [下一集预取] 完成: %s | 无新集需要预取（后续集已全部缓存或已是最后一集）", curName)
	}
}

// playbackProbeRetryInterval 返回PlaybackInfo probe轮询间隔
// 前段高频抢首播，后段降频避免长期占用资源。
func playbackProbeRetryInterval(attempt int) time.Duration {
	if attempt <= 10 {
		return 500 * time.Millisecond
	}
	if attempt <= 20 {
		return 1 * time.Second
	}
	return 2 * time.Second
}

// prefetchElapsedSeconds 粗略估算详情页预取累计等待时间（用于日志展示）
func prefetchElapsedSeconds(attempt int) float64 {
	if attempt <= 1 {
		return 0
	}
	total := time.Duration(0)
	for i := 1; i < attempt; i++ {
		total += playbackProbeRetryInterval(i)
	}
	return total.Seconds()
}

// shouldSkipPrefetch 检查是否应该跳过预请求
// 不同触发点用不同 key 前缀和 TTL，避免互相阻止：
//   - detail:    详情页预请求，30秒去重（进入详情页才触发，不需要频繁）
//   - proactive: PlaybackInfo 触发的主动预请求，15秒去重（切换集数/版本时能重新触发）
//   - retry:     400 响应触发的重试，5秒去重（飞牛返回400后能较快重试，但不会无限重试）
// key 格式建议："{前缀}:{itemID}|{mediaSourceId}"
// 包含 mediaSourceId 是为了支持版本切换场景（同 itemID 不同版本不去重）
// 返回 true 表示应该跳过，false 表示可以继续（并已记录本次预请求时间）
func (s *Server) shouldSkipPrefetch(key string, ttl int64, logTag string) bool {
	if key == "" {
		return false
	}
	s.prefetchMu.Lock()
	defer s.prefetchMu.Unlock()
	now := time.Now().Unix()
	if lastTime, exists := s.prefetchRecent[key]; exists && now-lastTime < ttl {
		s.logger.Debug("⏭️ [%s] 跳过（%d秒内已请求）: key=%s", logTag, ttl, key)
		return true
	}
	s.prefetchRecent[key] = now
	// 清理过期项，避免 map 无限增长
	// 策略：超过 200 条时清理所有超过 120 秒的条目；
	//       如果仍超过 500 条（全部在 120s 内的高频场景），按时间排序强制淘汰最旧的条目
	if len(s.prefetchRecent) > 200 {
		// 第一轮：清理超过 120 秒的过期项
		for k, t := range s.prefetchRecent {
			if now-t > 120 {
				delete(s.prefetchRecent, k)
			}
		}
		// 第二轮：如果仍超过 500 条，强制清理最旧的条目降到 300
		if len(s.prefetchRecent) > 500 {
			type entry struct {
				k string
				t int64
			}
			entries := make([]entry, 0, len(s.prefetchRecent))
			for k, t := range s.prefetchRecent {
				entries = append(entries, entry{k, t})
			}
			// 按时间排序（最旧在前）
			sort.Slice(entries, func(i, j int) bool {
				return entries[i].t < entries[j].t
			})
			// 删除最旧的条目，降到 300
			toDelete := len(entries) - 300
			for i := 0; i < toDelete; i++ {
				delete(s.prefetchRecent, entries[i].k)
			}
		}
	}
	return false
}

// markPrefetchDone 仅设置去重标记（不检查是否已存在）
// 用于预取链路：跳过去重检查但仍设置标记，防止后续正常请求重复触发
func (s *Server) markPrefetchDone(key string) {
	if key == "" {
		return
	}
	s.prefetchMu.Lock()
	defer s.prefetchMu.Unlock()
	s.prefetchRecent[key] = time.Now().Unix()
}

// buildPrefetchKey 构造去重 key
// 格式："{prefix}:{itemID}|{mediaSourceId}"
// mediaSourceId 为空时格式为 "{prefix}:{itemID}|"
func buildPrefetchKey(prefix, itemID, mediaSourceID string) string {
	return prefix + ":" + itemID + "|" + mediaSourceID
}

// extractItemIDFromPath 从 URL 路径提取 ItemID（委托给 util 公共包）
// 路径格式：/emby/Items/{id}/PlaybackInfo 或 /emby/Users/{uid}/Items/{id}
func extractItemIDFromPath(path string) string {
	return util.ExtractItemIDFromPath(path)
}

// extractPlaybackInfoItemID 从 PlaybackInfo 响应体提取 ItemId
// 正常 PlaybackInfo 200响应也需要触发下一集预取，所以不能只依赖详情页预取链路。
func extractPlaybackInfoItemID(resp *http.Response, body []byte) string {
	if len(body) == 0 {
		if resp != nil && resp.Request != nil {
			return extractItemIDFromPath(resp.Request.URL.Path)
		}
		return ""
	}

	data := body
	if resp != nil && strings.EqualFold(resp.Header.Get("Content-Encoding"), "gzip") {
		gr, err := gzip.NewReader(bytes.NewReader(body))
		if err == nil {
			if decompressed, readErr := io.ReadAll(io.LimitReader(gr, 10*1024*1024)); readErr == nil {
				data = decompressed
			}
			gr.Close()
		}
	}

	var playbackInfo struct {
		ItemID string `json:"ItemId"`
	}
	if err := json.Unmarshal(data, &playbackInfo); err != nil {
		if resp != nil && resp.Request != nil {
			return extractItemIDFromPath(resp.Request.URL.Path)
		}
		return ""
	}
	if strings.TrimSpace(playbackInfo.ItemID) != "" {
		return strings.TrimSpace(playbackInfo.ItemID)
	}
	if resp != nil && resp.Request != nil {
		return extractItemIDFromPath(resp.Request.URL.Path)
	}
	return ""
}

// isPlaybackInfoRequest 检查是否是PlaybackInfo（增强版 - 支持更多格式）
func (s *Server) isPlaybackInfoRequest(req *http.Request) bool {
	if req.Method != "POST" && req.Method != "GET" {
		return false
	}

	path := req.URL.Path
	pathLower := strings.ToLower(path)

	playbackPatterns := []string{
		"/playbackinfo",
		"/playback-info",
		"/playback_info",
	}

	for _, pattern := range playbackPatterns {
		if strings.Contains(pathLower, pattern) {
			s.logger.Debug("📥 [PlaybackInfo] 检测到请求: %s %s", req.Method, path)
			return true
		}
	}

	return false
}

// loggingMiddleware 日志中间件（轻量级）
func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// 请求ID追踪：优先沿用客户端传入的，否则生成一个
		requestID := r.Header.Get("X-Request-Id")
		if requestID == "" {
			requestID = generateRequestID()
		}
		w.Header().Set("X-Request-Id", requestID)

		// 安全响应头
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-XSS-Protection", "1; mode=block")
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")

		if s.streamHandler.Handle(w, r) {
			s.logger.Debug("📤 [请求] %s %s 命中流处理 耗时: %v", r.Method, r.URL.Path, time.Since(start))
			return
		}

		next.ServeHTTP(w, r)

		// Debug级别记录请求日志（method/path/latency）
		s.logger.Debug("📤 [请求] %s %s 耗时: %v", r.Method, r.URL.Path, time.Since(start))
	})
}

// requestCounter 请求ID自增计数器
var requestCounter uint64

// generateRequestID 生成请求ID（纳秒时间戳+PID+自增序号，降低冲突概率）
func generateRequestID() string {
	n := atomic.AddUint64(&requestCounter, 1)
	return fmt.Sprintf("%d-%d-%d", time.Now().UnixNano(), os.Getpid(), n)
}

// errorHandler 错误处理中间件（增强版 - 防止重复WriteHeader）
func (s *Server) errorHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		safeW := &safeResponseWriter{
			ResponseWriter: w,
			written:        false,
		}

		defer func() {
			if err := recover(); err != nil {
				errStr := ""
				var asErr error
				if e, ok := err.(error); ok {
					asErr = e
					errStr = e.Error()
				} else if str, ok := err.(string); ok {
					errStr = str
				}

				// 优先用 errors.Is 识别 context.Canceled，再回退到字符串匹配识别网络中断
				isClientAbort := false
				if asErr != nil && errors.Is(asErr, context.Canceled) {
					isClientAbort = true
				}
				if !isClientAbort && (strings.Contains(errStr, "abort") ||
					strings.Contains(errStr, "connection reset") ||
					strings.Contains(errStr, "broken pipe") ||
					strings.Contains(errStr, "context canceled")) {
					isClientAbort = true
				}

				if isClientAbort {
					s.logger.Debug("⚠️ 客户端中断: %s %s (%s)", r.Method, r.URL.Path, errStr)
				} else {
					s.logger.Error("❌ 请求异常: %s %s, Error=%v", r.Method, r.URL.Path, err)
					if !safeW.written {
						http.Error(safeW, "Internal Server Error", http.StatusInternalServerError)
					}
				}
			}
		}()

		next.ServeHTTP(safeW, r)
	})
}

// statsHandler 性能监控端点
func (s *Server) statsHandler(w http.ResponseWriter, r *http.Request) {
	// 限制仅允许 GET 方法
	if r.Method != "GET" {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte(`{"error":"method not allowed"}`))
		return
	}

	// 可选的token认证：若配置了 stats_token 则要求 ?token=xxx 查询参数
	// 使用常量时间比较，防止时序攻击泄露 token
	if token := s.config.GetStatsToken(); token != "" {
		userToken := r.URL.Query().Get("token")
		if subtle.ConstantTimeCompare([]byte(userToken), []byte(token)) != 1 {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
	} else {
		// 未配置 token 时，仅允许 localhost 访问，避免运行时信息外泄
		host, _, _ := net.SplitHostPort(r.RemoteAddr)
		if host != "" && host != "127.0.0.1" && host != "::1" && host != "localhost" {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte(`{"error":"forbidden: stats endpoint requires token for remote access"}`))
			return
		}
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	stats := map[string]interface{}{
		"version":   s.version,
		"timestamp": time.Now().Unix(),
		"uptime":    time.Since(s.startTime).String(),
	}

	if s.cache != nil {
		cacheStats := s.cache.GetStats()
		stats["cache"] = cacheStats
	}

	if s.streamHandler != nil {
		streamStats := s.streamHandler.GetStats()
		stats["stream"] = streamStats
	}

	jsonBytes, _ := json.Marshal(stats)
	w.Write(jsonBytes)
}

// healthHandler 健康检查端点
func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	// 限制仅允许 GET 方法
	if r.Method != "GET" {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte(`{"error":"method not allowed"}`))
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	health := map[string]string{
		"status": "ok",
		"uptime": time.Since(s.startTime).String(),
	}
	jsonBytes, _ := json.Marshal(health)
	w.Write(jsonBytes)
}

// safeResponseWriter 安全的响应写入器（防止重复WriteHeader）
type safeResponseWriter struct {
	http.ResponseWriter
	written bool
	header  bool
}

func (w *safeResponseWriter) WriteHeader(code int) {
	if !w.header {
		w.header = true
		// 标记已写入，确保 panic 恢复时不会再次 WriteHeader 导致
		// "200 状态码 + Internal Server Error body" 的不一致响应
		w.written = true
		w.ResponseWriter.WriteHeader(code)
	}
}

func (w *safeResponseWriter) Write(b []byte) (int, error) {
	w.written = true
	return w.ResponseWriter.Write(b)
}

func (w *safeResponseWriter) WriteHeaderNow() bool {
	return w.header
}
