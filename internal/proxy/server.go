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
	proxyMu         sync.RWMutex
	currentTarget   string
	startTime       time.Time
	httpServer      *http.Server
	stopOnce        sync.Once
	retryClient     *http.Client
	targetURL       *url.URL
	prefetchRecent  map[string]int64
	prefetchMu      sync.Mutex
	version         string
	// 批量预取相关组件
	authStore       *AuthStore
	batchPrefetcher *BatchPrefetcher
	posterPrefetch  *PosterPrefetcher
	libraryScanner  *LibraryScanner
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

	// 主动重试客户端
	retryTransport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          50,
		MaxIdleConnsPerHost:   10,
		MaxConnsPerHost:       20,
		IdleConnTimeout:       60 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 500 * time.Millisecond,
		ResponseHeaderTimeout: 10 * time.Second,
		DisableKeepAlives:     false,
		ForceAttemptHTTP2:     true,
	}
	retryClient := &http.Client{
		Timeout:   20 * time.Second,
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

	// 下一集预取回调绑定
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
	sh.SetMediaSourceMissHandler(s.prefetchForMediaSourceMiss)

	// 初始化批量预取组件
	initialConcurrency := cfg.GetPosterPrefetchConcurrency()
	if scanConc := cfg.GetLibraryScanConcurrency(); scanConc > initialConcurrency {
		initialConcurrency = scanConc
	}
	s.authStore = NewAuthStore()

	// 主动登录
	fnosUser := os.Getenv("FNOS_USERNAME")
	fnosPass := os.Getenv("FNOS_PASSWORD")
	if fnosUser != "" && fnosPass != "" {
		serverAddr := targetURL.Scheme + "://" + targetURL.Host
		if err := s.authStore.LoginViaEmby(fnosUser, fnosPass, serverAddr); err != nil {
			log.Warn("⚠️ [主动登录] 飞牛影视登录失败，将依赖被动捕获: %v", err)
		} else {
			log.Info("✅ [主动登录] 飞牛影视登录成功，UserID=%s", s.authStore.userID)
		}
	} else {
		log.Info("ℹ️ [主动登录] 未配置 FNOS_USERNAME/FNOS_PASSWORD，依赖被动捕获")
	}

	s.batchPrefetcher = NewBatchPrefetcher(s, initialConcurrency)
	s.posterPrefetch = NewPosterPrefetcher(s, s.batchPrefetcher, s.authStore)
	s.libraryScanner = NewLibraryScanner(s, s.batchPrefetcher, s.authStore)
	s.logger.Info("📦 [批量预取] 引擎已初始化: 并发=%d (海报墙=%d, 全库扫描=%d)",
		initialConcurrency, cfg.GetPosterPrefetchConcurrency(), cfg.GetLibraryScanConcurrency())

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
	mux.HandleFunc("/stats", s.statsHandler)
	mux.HandleFunc("/health", s.healthHandler)

	s.httpServer = &http.Server{
		Addr:              s.config.GetListenAddr(),
		Handler:           mux,
		ReadTimeout:       30 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       180 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	s.logger.Info("🌐 反代服务启动 (v%s): http://localhost%s", s.version, s.config.GetListenAddr())
	s.logger.Info("⏭️ [下一集预取] 当前运行版本包含：详情页成功显式触发 + STRM缓存回调触发")
	s.logger.Info("📊 性能监控: http://localhost%s/stats", s.config.GetListenAddr())

	// 启动全库扫描器
	if s.libraryScanner != nil {
		s.libraryScanner.Start()
	}

	return s.httpServer.ListenAndServe()
}

// setupProxy 配置反向代理的Director和ModifyResponse
func (s *Server) setupProxy(targetURL *url.URL) {
	originalDirector := s.proxy.Director
	s.proxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.Host = targetURL.Host

		// 捕获认证信息
		if s.authStore != nil {
			s.authStore.CaptureFromRequest(req)
		}

		// 进入详情页就主动预请求 PlaybackInfo
		if itemID, userID, ok := s.isItemDetailRequest(req); ok {
			go s.prefetchForDetailPage(req, itemID, userID)
			return
		}

		// 兜底：点击播放时的主动预请求
		if s.isPlaybackInfoRequest(req) && req.Method == "GET" {
			go s.proactivePlaybackInfo(req)
		}

		// ✅ 拦截 FNOS 原生海报墙请求，触发单库扫描
		if req.Method == "POST" && strings.HasSuffix(req.URL.Path, "/v/api/v1/item/list") {
			body, err := io.ReadAll(req.Body)
			if err == nil && len(body) > 0 {
				req.Body.Close()
				req.Body = io.NopCloser(bytes.NewReader(body))
				req.ContentLength = int64(len(body))
				req.Header.Set("Content-Length", strconv.Itoa(len(body)))
				if s.posterPrefetch != nil {
					go s.posterPrefetch.HandleFnosListRequest(body)
				}
			}
		}
	}
	s.proxy.ModifyResponse = s.handleResponse
	s.proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
		if errors.Is(err, context.Canceled) {
			s.logger.Debug("🔌 [客户端断开] %s %s: %v", req.Method, req.URL.Path, err)
			http.Error(rw, "Client closed request", 499)
			return
		}
		errStr := err.Error()
		if strings.Contains(errStr, "broken pipe") ||
			strings.Contains(errStr, "connection reset") ||
			strings.Contains(errStr, "EOF") {
			s.logger.Debug("🔌 [网络中断] %s %s: %v", req.Method, req.URL.Path, err)
			http.Error(rw, "Connection interrupted", 499)
			return
		}
		s.logger.Warn("⚠️ [代理错误] %s %s: %v", req.Method, req.URL.Path, err)
		http.Error(rw, "Bad Gateway", http.StatusBadGateway)
	}
}

// handleProxy 加锁读取代理实例并转发请求
func (s *Server) handleProxy(w http.ResponseWriter, r *http.Request) {
	s.proxyMu.RLock()
	proxy := s.proxy
	s.proxyMu.RUnlock()
	proxy.ServeHTTP(w, r)
}

// Stop 停止服务器
func (s *Server) Stop() error {
	var err error
	s.stopOnce.Do(func() {
		s.logger.Info("🛑 正在关闭服务...")

		if s.libraryScanner != nil {
			s.libraryScanner.Stop()
		}
		if s.batchPrefetcher != nil {
			s.batchPrefetcher.Stop()
		}
		if s.streamHandler != nil {
			s.streamHandler.Stop()
		}
		if s.httpServer != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err = s.httpServer.Shutdown(ctx)
			cancel()
		}
		if s.cache != nil {
			s.cache.Stop()
		}
		s.logger.Info("🔚 正在关闭日志系统...")
		s.logger.Close()
	})
	return err
}

func (s *Server) GetCache() *cache.Cache                    { return s.cache }
func (s *Server) GetLogger() *logger.Logger                  { return s.logger }
func (s *Server) GetStreamHandler() *handler.StreamHandler   { return s.streamHandler }
func (s *Server) GetLibraryScanner() *LibraryScanner         { return s.libraryScanner }

// Reload 重新加载配置
func (s *Server) Reload() {
	newTarget := s.config.GetTargetAddr()

	s.logger.SetLevel(s.config.GetLogLevel())
	s.logger.Info("🔄 配置已重载")
	s.logger.Info("   新日志级别: %s", s.config.GetLogLevel())

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

			if s.authStore != nil {
				s.authStore.CaptureFromRequest(req)
			}

			if itemID, userID, ok := s.isItemDetailRequest(req); ok {
				go s.prefetchForDetailPage(req, itemID, userID)
				return
			}
			if s.isPlaybackInfoRequest(req) && req.Method == "GET" {
				go s.proactivePlaybackInfo(req)
			}

			// ✅ FNOS 海报墙拦截（Reload 后同样生效）
			if req.Method == "POST" && strings.HasSuffix(req.URL.Path, "/v/api/v1/item/list") {
				body, err := io.ReadAll(req.Body)
				if err == nil && len(body) > 0 {
					req.Body.Close()
					req.Body = io.NopCloser(bytes.NewReader(body))
					req.ContentLength = int64(len(body))
					req.Header.Set("Content-Length", strconv.Itoa(len(body)))
					if s.posterPrefetch != nil {
						go s.posterPrefetch.HandleFnosListRequest(body)
					}
				}
			}
		}
		newProxy.ModifyResponse = s.handleResponse

		s.proxyMu.Lock()
		s.proxy = newProxy
		s.currentTarget = newTarget
		s.targetURL = targetURL
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
	// ✅ 拦截 Emby Views 响应，缓存媒体库列表（用于 FNOS→Emby 映射）
	if s.posterPrefetch != nil && resp.StatusCode == http.StatusOK &&
		resp.Request != nil && strings.HasSuffix(resp.Request.URL.Path, "/Views") {
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1*1024*1024))
		if err == nil {
			resp.Body.Close()
			resp.Body = io.NopCloser(bytes.NewBuffer(body))
			resp.ContentLength = int64(len(body))
			resp.Header.Set("Content-Length", strconv.Itoa(len(body)))
			resp.Header.Del("Transfer-Encoding")
			s.posterPrefetch.CacheEmbyLibraries(body)
		}
		return nil
	}

	// ✅ 拦截 FNOS 媒体库列表响应，缓存媒体库列表
	if s.posterPrefetch != nil && resp.StatusCode == http.StatusOK &&
		resp.Request != nil && strings.HasSuffix(resp.Request.URL.Path, "/v/api/v1/mediadb/list") {
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1*1024*1024))
		if err == nil {
			resp.Body.Close()
			resp.Body = io.NopCloser(bytes.NewBuffer(body))
			resp.ContentLength = int64(len(body))
			resp.Header.Set("Content-Length", strconv.Itoa(len(body)))
			resp.Header.Del("Transfer-Encoding")
			s.posterPrefetch.CacheFnosLibraries(body)
		}
		return nil
	}

	// 海报墙列表响应拦截：提取 ItemID 批量预取 Movie PlaybackInfo
	if s.posterPrefetch != nil && resp.StatusCode == http.StatusOK {
		if userID, ok := s.posterPrefetch.IsItemListRequest(resp.Request); ok {
			body, err := io.ReadAll(io.LimitReader(resp.Body, 20*1024*1024))
			if err != nil {
				s.logger.Warn("🖼️ [海报墙预取] 读取响应体失败: %v", err)
				return err
			}
			resp.Body.Close()
			resp.Body = io.NopCloser(bytes.NewBuffer(body))
			resp.ContentLength = int64(len(body))
			resp.Header.Set("Content-Length", strconv.Itoa(len(body)))
			resp.Header.Del("Transfer-Encoding")
			go s.posterPrefetch.HandleListResponse(resp, body, userID)
			return nil
		}
	}

	if !s.isPlaybackInfoRequest(resp.Request) {
		return nil
	}

	if resp.StatusCode != http.StatusOK {
		s.logger.Info("⚠️ [PlaybackInfo] %d 响应，启动重试: %s",
			resp.StatusCode, resp.Request.URL.Path)
		go s.retryPlaybackInfo(resp.Request)
		return nil
	}

	s.logger.Debug("📥 [PlaybackInfo] 200 响应: %s", resp.Request.URL.Path)

	body, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
	if err != nil {
		s.logger.Error("❌ [响应处理] 读取响应体失败: %v", err)
		return err
	}
	resp.Body.Close()

	if len(body) == 0 {
		s.logger.Debug("📊 [响应处理] 响应体为空，跳过处理")
		return nil
	}

	s.logger.Debug("📊 [响应处理] 响应体大小: %d bytes", len(body))

	newBody, err := s.playbackHandler.Handle(resp, body)
	if err != nil {
		s.logger.Error("❌ [响应处理] PlaybackInfo处理失败: %v", err)
	}

	resp.ContentLength = int64(len(newBody))
	resp.Header.Set("Content-Length", strconv.Itoa(len(newBody)))
	resp.Header.Del("Transfer-Encoding")
	resp.Body = io.NopCloser(bytes.NewBuffer(newBody))

	s.logger.Debug("✅ [响应处理] PlaybackInfo处理完成")
	return nil
}

// retryPlaybackInfo 主动重试 PlaybackInfo 请求
func (s *Server) retryPlaybackInfo(originalReq *http.Request) {
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
	reqHeaders.Del("Accept-Encoding")
	reqHeaders.Set("Accept", "application/json")

	time.Sleep(100 * time.Millisecond)

	s.logger.Info("🔄 [重试] 开始: %s", reqPath)

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
		return
	}

	s.logger.Debug("⚠️ [重试] %d次尝试均未成功(飞牛probe超时): %s", maxAttempts, reqPath)
}

// proactivePlaybackInfo 主动预请求
func (s *Server) proactivePlaybackInfo(originalReq *http.Request) {
	itemID := extractItemIDFromPath(originalReq.URL.Path)
	mediaSourceID := originalReq.URL.Query().Get("MediaSourceId")

	if itemID != "" {
		if source, found := s.cache.GetByItemID(itemID); found {
			if _, urlFound := s.cache.GetStreamURL(source.ID); urlFound {
				s.logger.Debug("⏭️ [主动预请求] 跳过（已缓存且直链已解析）: ItemId=%s", itemID)
				return
			}
		}
	}
	if s.shouldSkipPrefetch(buildPrefetchKey("proactive", itemID, mediaSourceID), 3, "主动预请求") {
		return
	}

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
	reqHeaders.Del("Accept-Encoding")
	reqHeaders.Set("Accept", "application/json")

	time.Sleep(100 * time.Millisecond)

	s.logger.Info("🚀 [主动] 开始: %s", reqPath)

	req, err := http.NewRequestWithContext(context.Background(), reqMethod, reqURL, nil)
	if err != nil {
		s.logger.Warn("❌ [主动] 创建请求失败: %v", err)
		return
	}

	req.Header = reqHeaders.Clone()
	req.Host = reqHost

	resp, err := s.retryClient.Do(req)
	if err != nil {
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

	s.logger.Info("✅ [主动] 成功: %s", reqPath)
	_, _ = s.playbackHandler.Handle(resp, body)
}

// isItemDetailRequest 检查是否是详情页请求
func (s *Server) isItemDetailRequest(req *http.Request) (string, string, bool) {
	if req.Method != "GET" {
		return "", "", false
	}

	path := req.URL.Path
	pathLower := strings.ToLower(path)
	pathLower = strings.TrimPrefix(pathLower, "/emby")
	pathLower = strings.TrimPrefix(pathLower, "/")

	parts := strings.Split(pathLower, "/")
	if len(parts) < 2 {
		return "", "", false
	}

	itemID := parts[len(parts)-1]
	if len(itemID) < 16 || !util.IsGUIDLikeLoose(itemID) {
		return "", "", false
	}

	itemsIdx := len(parts) - 2
	if parts[itemsIdx] != "items" {
		return "", "", false
	}

	userID := ""
	if itemsIdx >= 2 && parts[itemsIdx-2] == "users" {
		uidCandidate := parts[itemsIdx-1]
		if len(uidCandidate) >= 16 && util.IsGUIDLikeLoose(uidCandidate) {
			userID = uidCandidate
		}
	}

	s.logger.Debug("🎬 [详情页检测] 命中: %s %s (ItemId=%s, UserId=%s)", req.Method, path, itemID, userID)
	return itemID, userID, true
}

// prefetchForDetailPage 进入详情页时主动预请求 PlaybackInfo
func (s *Server) prefetchForDetailPage(originalReq *http.Request, itemID string, userID string) {
	isFromPrefetch := originalReq.Header.Get("X-Fnysfd-Next-Prefetch") == "1"
	if !isFromPrefetch {
		if s.shouldSkipPrefetch(buildPrefetchKey("detail", itemID, ""), 30, "详情页预请求") {
			return
		}
	} else {
		s.markPrefetchDone(buildPrefetchKey("detail", itemID, ""))
		s.logger.Debug("🎬 [详情页] 预取链路调用，跳过检查但设置去重标记: %s", itemID)
	}

	s.proxyMu.RLock()
	targetURL := s.targetURL
	s.proxyMu.RUnlock()

	playbackPath := "/emby/Items/" + itemID + "/PlaybackInfo"

	query := originalReq.URL.Query()
	if userID != "" && query.Get("UserId") == "" {
		query.Set("UserId", userID)
	}

	fullURL := targetURL.Scheme + "://" + targetURL.Host + playbackPath
	if encoded := query.Encode(); encoded != "" {
		fullURL += "?" + encoded
	}

	reqHeaders := originalReq.Header.Clone()
	reqHeaders.Del("Accept-Encoding")
	reqHeaders.Set("Accept", "application/json")
	reqHost := targetURL.Host

	time.Sleep(50 * time.Millisecond)

	s.logger.Info("🎬 [详情页] 开始: %s", itemID)

	const maxAttempts = 30
	for attempt := 1; attempt <= maxAttempts; attempt++ {
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

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			s.logger.Debug("⏳ [详情页] 第%d次: %d (飞牛probe中, 已等%ds): %s",
				attempt, resp.StatusCode, int(prefetchElapsedSeconds(attempt)), itemID)
			if attempt < maxAttempts {
				time.Sleep(playbackProbeRetryInterval(attempt))
			}
			continue
		}

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

		if attempt == 1 {
			s.logger.Info("✅ [详情页] 首次成功: %s", itemID)
		} else {
			s.logger.Info("✅ [详情页] 第%d次成功(约等%.1fs): %s (飞牛probe完成)",
				attempt, prefetchElapsedSeconds(attempt), itemID)
		}
		_, _ = s.playbackHandler.Handle(resp, body)
		return
	}

	s.logger.Debug("⚠️ [详情页] %d次尝试均未成功(飞牛probe超时约33s): %s", maxAttempts, itemID)
}

// prefetchForMediaSourceMiss 流请求发现 MediaSource 未缓存时的同步兜底
func (s *Server) prefetchForMediaSourceMiss(originalReq *http.Request, itemID string, mediaSourceID string) bool {
	if itemID == "" {
		return false
	}

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
	reqHeaders.Del("Accept-Encoding")
	reqHeaders.Set("Accept", "application/json")
	reqHost := targetURL.Host

	s.logger.Info("🧩 [MediaSource兜底] 轮询PlaybackInfo: ItemId=%s, MS=%s", itemID, mediaSourceID)

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
			s.logger.Info("✅ [MediaSource兜底] 第%d次成功: ItemId=%s, 等待URL解析...", attempt, itemID)
			if s.streamHandler.WaitForURLResolution(cachedMediaSourceID, itemID, 5*time.Second) {
				s.logger.Info("✅ [MediaSource兜底] URL已就绪: ItemId=%s", itemID)
			} else {
				s.logger.Warn("⚠️ [MediaSource兜底] URL解析未完成，流请求将自行解析: ItemId=%s", itemID)
			}
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
	reqHeaders.Del("Accept-Encoding")
	reqHeaders.Set("Accept", "application/json")
	reqHost := targetURL.Host

	var detailURL string
	query := originalReq.URL.Query()
	if userID != "" {
		detailURL = targetURL.Scheme + "://" + targetURL.Host + "/emby/Users/" + userID + "/Items/" + itemID
	} else {
		detailURL = targetURL.Scheme + "://" + targetURL.Host + "/emby/Items/" + itemID
	}
	if len(query) > 0 {
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
		Name        string `json:"Name"`
		ParentID    string `json:"ParentId"`
		SeriesID    string `json:"SeriesId"`
		SeasonID    string `json:"SeasonId"`
		IndexNumber int    `json:"IndexNumber"`
		Type        string `json:"Type"`
	}
	if err := json.Unmarshal(body, &detail); err != nil {
		preview := string(body)
		if len(preview) > 200 {
			preview = preview[:200]
		}
		s.logger.Warn("⏭️ [下一集预取] 当前集详情JSON解析失败: %v itemID=%s bodyLen=%d bodyPreview=%s", err, itemID, len(body), preview)
		return
	}
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
	curName := detail.Name
	if curName == "" {
		curName = itemID
	}
	s.logger.Info("⏭️ [下一集预取] 当前集: %s (第%d集)，准备查询同季列表", curName, detail.IndexNumber)
	s.logger.Debug("⏭️ [下一集预取] 当前集信息: ParentId=%s SeriesId=%s SeasonId=%s Index=%d",
		detail.ParentID, detail.SeriesID, detail.SeasonID, detail.IndexNumber)

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
			Name        string `json:"Name"`
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

	curPos := -1
	for i, ep := range listResp.Items {
		if ep.ID == itemID {
			curPos = i
			break
		}
	}

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
		shouldSkip := false
		if detail.IndexNumber > 0 && ep.IndexNumber > 0 {
			if ep.IndexNumber <= detail.IndexNumber {
				shouldSkip = true
			}
		} else if curPos >= 0 {
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

	if prefetched > 0 {
		s.logger.Info("⏭️ [下一集预取] 完成: %s | 新预取 %d 集, 已缓存 %d 集", curName, prefetched, alreadyCached)
	} else {
		s.logger.Info("⏭️ [下一集预取] 完成: %s | 无新集需要预取（后续集已全部缓存或已是最后一集）", curName)
	}
}

func playbackProbeRetryInterval(attempt int) time.Duration {
	if attempt <= 10 {
		return 500 * time.Millisecond
	}
	if attempt <= 20 {
		return 1 * time.Second
	}
	return 2 * time.Second
}

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
	if len(s.prefetchRecent) > 200 {
		for k, t := range s.prefetchRecent {
			if now-t > 120 {
				delete(s.prefetchRecent, k)
			}
		}
		if len(s.prefetchRecent) > 500 {
			type entry struct {
				k string
				t int64
			}
			entries := make([]entry, 0, len(s.prefetchRecent))
			for k, t := range s.prefetchRecent {
				entries = append(entries, entry{k, t})
			}
			sort.Slice(entries, func(i, j int) bool {
				return entries[i].t < entries[j].t
			})
			toDelete := len(entries) - 300
			for i := 0; i < toDelete; i++ {
				delete(s.prefetchRecent, entries[i].k)
			}
		}
	}
	return false
}

// markPrefetchDone 仅设置去重标记
func (s *Server) markPrefetchDone(key string) {
	if key == "" {
		return
	}
	s.prefetchMu.Lock()
	defer s.prefetchMu.Unlock()
	s.prefetchRecent[key] = time.Now().Unix()
}

// buildPrefetchKey 构造去重 key
func buildPrefetchKey(prefix, itemID, mediaSourceID string) string {
	return prefix + ":" + itemID + "|" + mediaSourceID
}

// extractItemIDFromPath 从 URL 路径提取 ItemID
func extractItemIDFromPath(path string) string {
	return util.ExtractItemIDFromPath(path)
}

// extractPlaybackInfoItemID 从 PlaybackInfo 响应体提取 ItemId
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

// isPlaybackInfoRequest 检查是否是PlaybackInfo
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

// loggingMiddleware 日志中间件
func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		requestID := r.Header.Get("X-Request-Id")
		if requestID == "" {
			requestID = generateRequestID()
		}
		w.Header().Set("X-Request-Id", requestID)

		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-XSS-Protection", "1; mode=block")
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")

		if s.streamHandler.Handle(w, r) {
			s.logger.Debug("📤 [请求] %s %s 命中流处理 耗时: %v", r.Method, r.URL.Path, time.Since(start))
			return
		}

		next.ServeHTTP(w, r)

		s.logger.Debug("📤 [请求] %s %s 耗时: %v", r.Method, r.URL.Path, time.Since(start))
	})
}

var requestCounter uint64

func generateRequestID() string {
	n := atomic.AddUint64(&requestCounter, 1)
	return fmt.Sprintf("%d-%d-%d", time.Now().UnixNano(), os.Getpid(), n)
}

// errorHandler 错误处理中间件
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
	if r.Method != "GET" {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte(`{"error":"method not allowed"}`))
		return
	}

	if token := s.config.GetStatsToken(); token != "" {
		userToken := r.URL.Query().Get("token")
		if subtle.ConstantTimeCompare([]byte(userToken), []byte(token)) != 1 {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
	} else {
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

// safeResponseWriter 安全的响应写入器
type safeResponseWriter struct {
	http.ResponseWriter
	written bool
	header  bool
}

func (w *safeResponseWriter) WriteHeader(code int) {
	if !w.header {
		w.header = true
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
