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
	prefetchClient  *http.Client // 批量预取专用（长超时）
	targetURL       *url.URL
	prefetchRecent  map[string]int64
	prefetchMu      sync.Mutex
	version         string
	// 批量预取相关组件
	authStore       *AuthStore
	batchPrefetcher *BatchPrefetcher
	posterPrefetch  *PosterPrefetcher
	libraryScanner  *LibraryScanner
	doubanProvider  *DoubanProvider // ✅ 新增

	// ✅ TV guid → title 缓存（季列表注入需要）
	seriesTitleCache sync.Map

	// 服务级 context：Stop 时取消，用于中断内部长任务
	serverCtx    context.Context
	serverCancel context.CancelFunc
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

	// 主动重试客户端（实时路径用）
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

	// 批量预取专用客户端（长超时）
	prefetchTransport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          20,
		MaxIdleConnsPerHost:   5,
		MaxConnsPerHost:       10,
		IdleConnTimeout:       120 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 90 * time.Second,
		DisableKeepAlives:     false,
		ForceAttemptHTTP2:     true,
	}
	prefetchClient := &http.Client{
		Timeout:   120 * time.Second,
		Transport: prefetchTransport,
	}

	// 服务级 context
	serverCtx, serverCancel := context.WithCancel(context.Background())

	s := &Server{
		config:          cfg,
		logger:          log,
		cache:           c,
		playbackHandler: ph,
		streamHandler:   sh,
		proxy:           proxy,
		currentTarget:   cfg.GetTargetAddr(),
		retryClient:     retryClient,
		prefetchClient:  prefetchClient,
		targetURL:       targetURL,
		prefetchRecent:  make(map[string]int64),
		version:         version,
		serverCtx:       serverCtx,
		serverCancel:    serverCancel,
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

		// ✅ 克隆请求再传给 goroutine
		reqCopy := resp.Request.Clone(context.Background())
		userID := reqCopy.URL.Query().Get("UserId")
		go s.prefetchNextEpisodesBestEffort(reqCopy, itemID, userID)
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
			_, uid, _ := s.authStore.Get()
			log.Info("✅ [主动登录] 飞牛影视登录成功，UserID=%s", uid)
		}
	} else {
		log.Info("ℹ️ [主动登录] 未配置 FNOS_USERNAME/FNOS_PASSWORD，依赖被动捕获")
	}

	s.batchPrefetcher = NewBatchPrefetcher(s, initialConcurrency)
	s.posterPrefetch = NewPosterPrefetcher(s, s.batchPrefetcher, s.authStore)
	s.doubanProvider = NewDoubanProvider(s) // ✅ 新增
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

// applyProxyHandlers 给 proxy 挂上 Director / ModifyResponse / ErrorHandler
func (s *Server) applyProxyHandlers(p *httputil.ReverseProxy, targetURL *url.URL) {
	originalDirector := p.Director
	p.Director = func(req *http.Request) {
		originalDirector(req)
		req.Host = targetURL.Host

		// 捕获认证信息
		if s.authStore != nil {
			s.authStore.CaptureFromRequest(req)
		}

		// 进入详情页就主动预请求 PlaybackInfo
		if itemID, userID, ok := s.isItemDetailRequest(req); ok {
			reqCopy := req.Clone(context.Background())
			go s.prefetchForDetailPage(reqCopy, itemID, userID)
			return
		}

		// 兜底：点击播放时的主动预请求
		if s.isPlaybackInfoRequest(req) && req.Method == "GET" {
			reqCopy := req.Clone(context.Background())
			go s.proactivePlaybackInfo(reqCopy)
		}

		// 拦截 FNOS 原生海报墙请求
		if req.Method == "POST" && strings.HasSuffix(req.URL.Path, "/v/api/v1/item/list") {
			body, err := io.ReadAll(io.LimitReader(req.Body, 1*1024*1024))
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
	p.ModifyResponse = s.handleResponse
	p.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
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

// setupProxy 配置反向代理
func (s *Server) setupProxy(targetURL *url.URL) {
	s.applyProxyHandlers(s.proxy, targetURL)
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

		// 0. 取消服务级 context
		if s.serverCancel != nil {
			s.serverCancel()
		}

		// 1. 先停止接收新请求
		if s.httpServer != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err = s.httpServer.Shutdown(ctx)
			cancel()
		}

		// 2. 停止后台任务
		if s.libraryScanner != nil {
			s.libraryScanner.Stop()
		}
		if s.doubanProvider != nil { // ✅ 新增
			s.doubanProvider.Stop()
		}
		if s.batchPrefetcher != nil {
			s.batchPrefetcher.Stop()
		}
		if s.streamHandler != nil {
			s.streamHandler.Stop()
		}

		// 3. 最后关 cache 和 logger
		if s.cache != nil {
			s.cache.Stop()
		}
		s.logger.Info("🔚 正在关闭日志系统...")
		s.logger.Close()
	})
	return err
}

func (s *Server) GetCache() *cache.Cache                   { return s.cache }
func (s *Server) GetLogger() *logger.Logger                { return s.logger }
func (s *Server) GetStreamHandler() *handler.StreamHandler { return s.streamHandler }
func (s *Server) GetLibraryScanner() *LibraryScanner       { return s.libraryScanner }
func (s *Server) GetDoubanProvider() *DoubanProvider       { return s.doubanProvider } // ✅ 新增

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
		s.applyProxyHandlers(newProxy, targetURL)

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

// injectDoubanRatings 解析 JSON，对每个 item 注入豆瓣评分
//
// ✅ 新增：豆瓣评分注入（Emby 协议 /Items 响应）
func (s *Server) injectDoubanRatings(body []byte) []byte {
	var obj map[string]interface{}
	if err := json.Unmarshal(body, &obj); err != nil {
		return body
	}

	injected := false

	// 列表响应
	if items, ok := obj["Items"].([]interface{}); ok {
		for _, it := range items {
			if m, ok := it.(map[string]interface{}); ok {
				s.doubanProvider.InjectInto(m)
				injected = true
			}
		}
	} else if _, ok := obj["Id"]; ok {
		// 单项响应
		s.doubanProvider.InjectInto(obj)
		injected = true
	}

	if !injected {
		return body
	}

	newBody, err := json.Marshal(obj)
	if err != nil {
		return body
	}
	return newBody
}

// ============================================================
// ✅ 飞牛原生详情接口注入（/v/api/v1/item/{guid}）
// ============================================================

// shouldInjectNative 判断是否飞牛原生详情接口 /v/api/v1/item/{guid}
func (s *Server) shouldInjectNative(path string) bool {
	const prefix = "/v/api/v1/item/"
	if !strings.HasPrefix(path, prefix) {
		return false
	}
	rest := path[len(prefix):]
	// 必须正好是一个 32 位 GUID，后面不带任何子路径
	if rest == "" || strings.Contains(rest, "/") || len(rest) != 32 {
		return false
	}
	return true
}

// injectDoubanNative 注入豆瓣评分到飞牛原生详情响应
//
// 响应结构：
//   { "code":0, "data": { "imdb_id":"ttxxx", "title":"xxx", "type":"Movie",
//                         "vote_average":"7.2535...", ... } }
//
// type 说明：
//   - "Movie"          → 用 IMDb 查电影评分
//   - "Series" / "TV"  → 剧集，用 number_of_seasons / season_number 查最新季豆瓣分
//                       同时缓存 guid → title（供季列表注入用）
//   - "Season"         → 用 tv_title（剧名）+ season_number 查季分
func (s *Server) injectDoubanNative(body []byte) []byte {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Warn("🎬 [豆瓣评分] 原生注入 panic: %v", r)
		}
	}()

	var payload map[string]interface{}
	if err := json.Unmarshal(body, &payload); err != nil {
		return body
	}

	// 只处理 code==0 的成功响应
	if code, ok := payload["code"].(float64); ok && code != 0 {
		return body
	}

	data, ok := payload["data"].(map[string]interface{})
	if !ok || data == nil {
		return body
	}

	imdbID, _ := data["imdb_id"].(string)
	title, _ := data["title"].(string)
	itemType, _ := data["type"].(string)

	// ✅ 缓存 TV guid → title（供季列表注入用）
	if itemType == "TV" || itemType == "Series" {
		if guid, ok := data["guid"].(string); ok && guid != "" && title != "" {
			s.seriesTitleCache.Store(guid, title)
		}
	}

	var rating float64
	var found bool

	switch itemType {
	case "Movie":
		rating, found = s.doubanProvider.GetRating(imdbID, title, 0)

	case "Series", "TV":
		// ✅ 剧集：优先用 season_number，其次 number_of_seasons，查最新季豆瓣分
		seriesName := title
		seasonNum := 0
		if n, ok := data["season_number"].(float64); ok && n > 0 {
			seasonNum = int(n)
		}
		if seasonNum == 0 {
			if n, ok := data["number_of_seasons"].(float64); ok && n > 0 {
				seasonNum = int(n)
			}
		}
		if seasonNum > 0 {
			rating, found = s.doubanProvider.GetSeasonRating(seriesName, seasonNum)
		}
		if !found {
			// 兜底：IMDb 查整剧（当前缓存里可能没有）
			rating, found = s.doubanProvider.GetRating(imdbID, title, 0)
		}
		// ✅ TV 详情页前端不渲染 vote_average，把评分附加到 content_ratings
		if found && rating > 0 {
			cr, _ := data["content_ratings"].(string)
			if !strings.Contains(cr, "⭐") {
				if cr == "" {
					data["content_ratings"] = fmt.Sprintf("⭐%.1f", rating)
				} else {
					data["content_ratings"] = fmt.Sprintf("%s · ⭐%.1f", cr, rating)
				}
			}
		}
		
	case "Season":
		// ✅ 剧名在 tv_title（飞牛的 parent_title 是空字符串）
		seriesName, _ := data["tv_title"].(string)
		if seriesName == "" {
			// 兜底：万一某些接口用 parent_title
			seriesName, _ = data["parent_title"].(string)
		}
		seasonNum := 0
		if n, ok := data["season_number"].(float64); ok {
			seasonNum = int(n)
		}
		if seriesName != "" && seasonNum > 0 {
			rating, found = s.doubanProvider.GetSeasonRating(seriesName, seasonNum)
		}
	}

	if !found || rating <= 0 {
		return body
	}

	// ✅ vote_average 是字符串，保持类型
	data["vote_average"] = fmt.Sprintf("%.1f", rating)
	s.logger.Debug("🎬 [豆瓣评分] 原生注入 %s (%s): vote_average → %.1f", title, itemType, rating)

	newBody, err := json.Marshal(payload)
	if err != nil {
		return body
	}
	return newBody
}

// ============================================================
// ✅ 飞牛原生列表接口注入（/v/api/v1/item/list）
//    海报墙评分（左上角数字）走这里
// ============================================================

// shouldInjectNativeList 判断是否飞牛原生列表接口（海报墙等）
func (s *Server) shouldInjectNativeList(path string) bool {
	return strings.HasSuffix(path, "/v/api/v1/item/list")
}

// injectDoubanNativeList 注入豆瓣评分到飞牛原生列表响应
//
// 响应结构：{ "code":0, "data": { "list": [ {..item..}, ... ] } }
//
// type 说明（与详情页一致）：
//   - "Movie"          → 用 IMDb 查电影评分
//   - "Series" / "TV"  → 剧集，用 number_of_seasons / season_number 查最新季豆瓣分
//   - "Season"         → 用 tv_title（剧名）+ season_number 查季分
func (s *Server) injectDoubanNativeList(body []byte) []byte {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Warn("🎬 [豆瓣评分] 原生列表注入 panic: %v", r)
		}
	}()

	var payload map[string]interface{}
	if err := json.Unmarshal(body, &payload); err != nil {
		return body
	}

	if code, ok := payload["code"].(float64); ok && code != 0 {
		return body
	}

	data, ok := payload["data"].(map[string]interface{})
	if !ok || data == nil {
		return body
	}

	list, ok := data["list"].([]interface{})
	if !ok || len(list) == 0 {
		return body
	}

	injected := 0
	for _, it := range list {
		item, ok := it.(map[string]interface{})
		if !ok {
			continue
		}

		imdbID, _ := item["imdb_id"].(string)
		title, _ := item["title"].(string)
		itemType, _ := item["type"].(string)

		var rating float64
		var found bool

		switch itemType {
		case "Movie":
			rating, found = s.doubanProvider.GetRating(imdbID, title, 0)

		case "Series", "TV":
			// ✅ 剧集：优先用 season_number，其次 number_of_seasons，查最新季豆瓣分
			seriesName := title
			seasonNum := 0
			if n, ok := item["season_number"].(float64); ok && n > 0 {
				seasonNum = int(n)
			}
			if seasonNum == 0 {
				if n, ok := item["number_of_seasons"].(float64); ok && n > 0 {
					seasonNum = int(n)
				}
			}
			if seasonNum > 0 {
				rating, found = s.doubanProvider.GetSeasonRating(seriesName, seasonNum)
			}
			if !found {
				rating, found = s.doubanProvider.GetRating(imdbID, title, 0)
			}

		case "Season":
			// ✅ 剧名在 tv_title（飞牛的 parent_title 是空字符串）
			seriesName, _ := item["tv_title"].(string)
			if seriesName == "" {
				seriesName, _ = item["parent_title"].(string)
			}
			seasonNum := 0
			if n, ok := item["season_number"].(float64); ok {
				seasonNum = int(n)
			}
			if seriesName != "" && seasonNum > 0 {
				rating, found = s.doubanProvider.GetSeasonRating(seriesName, seasonNum)
			}
		}

		if !found || rating <= 0 {
			continue
		}

		// ✅ vote_average 是字符串，保持类型
		item["vote_average"] = fmt.Sprintf("%.1f", rating)
		injected++
	}

	if injected == 0 {
		return body
	}

	s.logger.Debug("🎬 [豆瓣评分] 原生列表注入: %d 项", injected)

	newBody, err := json.Marshal(payload)
	if err != nil {
		return body
	}
	return newBody
}

// ============================================================
// ✅ 飞牛原生季列表接口注入（/v/api/v1/season/list/{TV_guid}）
//    TV 详情页下方横排的季小海报走这里
//
// 响应结构：{ "code":0, "data": [ {..season..}, ... ] }
//    注意：data 是数组，不是 {list: []}
//
// 剧名获取：响应里 tv_title / parent_title 都为空，只能靠 parent_guid
//           从 seriesTitleCache（TV 详情页注入时缓存）反查
// ============================================================

// shouldInjectNativeSeasonList 判断是否飞牛原生季列表接口
func (s *Server) shouldInjectNativeSeasonList(path string) bool {
	const prefix = "/v/api/v1/season/list/"
	if !strings.HasPrefix(path, prefix) {
		return false
	}
	rest := path[len(prefix):]
	// 必须正好是一个 32 位 GUID
	return rest != "" && !strings.Contains(rest, "/") && len(rest) == 32
}

// injectDoubanNativeSeasonList 注入豆瓣评分到飞牛原生季列表响应
func (s *Server) injectDoubanNativeSeasonList(body []byte) []byte {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Warn("🎬 [豆瓣评分] 原生季列表注入 panic: %v", r)
		}
	}()

	var payload map[string]interface{}
	if err := json.Unmarshal(body, &payload); err != nil {
		return body
	}

	if code, ok := payload["code"].(float64); ok && code != 0 {
		return body
	}

	// ✅ 注意：data 直接是数组
	list, ok := payload["data"].([]interface{})
	if !ok || len(list) == 0 {
		return body
	}

	injected := 0
	for _, it := range list {
		item, ok := it.(map[string]interface{})
		if !ok {
			continue
		}

		itemType, _ := item["type"].(string)
		if itemType != "Season" {
			continue
		}

		seasonNum := 0
		if n, ok := item["season_number"].(float64); ok {
			seasonNum = int(n)
		}
		if seasonNum <= 0 {
			continue
		}

		// 剧名：从 parent_guid 反查缓存
		parentGUID, _ := item["parent_guid"].(string)
		if parentGUID == "" {
			continue
		}
		titleVal, ok := s.seriesTitleCache.Load(parentGUID)
		if !ok {
			continue
		}
		seriesName, _ := titleVal.(string)
		if seriesName == "" {
			continue
		}

		rating, found := s.doubanProvider.GetSeasonRating(seriesName, seasonNum)
		if !found || rating <= 0 {
			continue
		}

		// ✅ vote_average 是字符串，保持类型
		item["vote_average"] = fmt.Sprintf("%.1f", rating)
		injected++
	}

	if injected == 0 {
		return body
	}

	s.logger.Debug("🎬 [豆瓣评分] 原生季列表注入: %d 项", injected)

	newBody, err := json.Marshal(payload)
	if err != nil {
		return body
	}
	return newBody
}

// handleResponse 处理响应
func (s *Server) handleResponse(resp *http.Response) error {
	// ============================================================
	// ✅ 飞牛原生详情接口注入：/v/api/v1/item/{guid}
	//    放在最前面，避免 resp.Body 被后续逻辑消费
	// ============================================================
	if s.doubanProvider != nil && resp.StatusCode == http.StatusOK && resp.Request != nil {
		nativePath := resp.Request.URL.Path
		if s.shouldInjectNative(nativePath) {
			body, err := io.ReadAll(io.LimitReader(resp.Body, 5*1024*1024))
			if err == nil && len(body) > 0 {
				resp.Body.Close()
				newBody := s.injectDoubanNative(body)
				resp.Body = io.NopCloser(bytes.NewBuffer(newBody))
				resp.ContentLength = int64(len(newBody))
				resp.Header.Set("Content-Length", strconv.Itoa(len(newBody)))
				resp.Header.Del("Transfer-Encoding")
				return nil
			}
		}
	}

	// ============================================================
	// ✅ 飞牛原生列表接口注入：/v/api/v1/item/list
	//    海报墙评分（左上角数字）走这里
	// ============================================================
	if s.doubanProvider != nil && resp.StatusCode == http.StatusOK && resp.Request != nil {
		listPath := resp.Request.URL.Path
		if s.shouldInjectNativeList(listPath) {
			body, err := io.ReadAll(io.LimitReader(resp.Body, 20*1024*1024))
			if err == nil && len(body) > 0 {
				resp.Body.Close()
				newBody := s.injectDoubanNativeList(body)
				resp.Body = io.NopCloser(bytes.NewBuffer(newBody))
				resp.ContentLength = int64(len(newBody))
				resp.Header.Set("Content-Length", strconv.Itoa(len(newBody)))
				resp.Header.Del("Transfer-Encoding")
				return nil
			}
		}
	}

	// ============================================================
	// ✅ 飞牛原生季列表接口注入：/v/api/v1/season/list/{TV_guid}
	//    TV 详情页下方横排的季小海报走这里
	// ============================================================
	if s.doubanProvider != nil && resp.StatusCode == http.StatusOK && resp.Request != nil {
		seasonListPath := resp.Request.URL.Path
		if s.shouldInjectNativeSeasonList(seasonListPath) {
			body, err := io.ReadAll(io.LimitReader(resp.Body, 5*1024*1024))
			if err == nil && len(body) > 0 {
				resp.Body.Close()
				newBody := s.injectDoubanNativeSeasonList(body)
				resp.Body = io.NopCloser(bytes.NewBuffer(newBody))
				resp.ContentLength = int64(len(newBody))
				resp.Header.Set("Content-Length", strconv.Itoa(len(newBody)))
				resp.Header.Del("Transfer-Encoding")
				return nil
			}
		}
	}

	// ============================================================
	// ✅ 豆瓣评分注入：拦截 /Items 响应（Emby 协议，列表或详情）
	// ============================================================
	if s.doubanProvider != nil && resp.StatusCode == http.StatusOK && resp.Request != nil {
		path := resp.Request.URL.Path
		if strings.Contains(path, "/Items") && !strings.Contains(path, "/PlaybackInfo") {
			body, err := io.ReadAll(io.LimitReader(resp.Body, 20*1024*1024))
			if err == nil && len(body) > 0 {
				resp.Body.Close()
				newBody := s.injectDoubanRatings(body)
				resp.Body = io.NopCloser(bytes.NewBuffer(newBody))
				resp.ContentLength = int64(len(newBody))
				resp.Header.Set("Content-Length", strconv.Itoa(len(newBody)))
				resp.Header.Del("Transfer-Encoding")
				return nil
			}
		}
	}

	// ✅ 拦截 Emby Views 响应，缓存媒体库列表
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

	// ✅ 拦截 FNOS 媒体库列表响应
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

	// ⚠️ 海报墙预取触发已临时禁用（方案 A）
	// if s.posterPrefetch != nil && resp.StatusCode == http.StatusOK {
	// 	if userID, ok := s.posterPrefetch.IsItemListRequest(resp.Request); ok {
	// 		...
	// 	}
	// }

	// PlaybackInfo 处理
	if resp.Request == nil {
		return nil
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

	overallCtx, overallCancel := context.WithTimeout(s.serverCtx, 60*time.Second)
	defer overallCancel()

	const maxAttempts = 5
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(overallCtx, reqMethod, reqURL, nil)
		if err != nil {
			s.logger.Warn("❌ [重试] 创建请求失败: %v", err)
			return
		}
		req.Header = reqHeaders.Clone()
		req.Host = reqHost

		resp, err := s.retryClient.Do(req)
		if err != nil {
			if s.serverCtx.Err() != nil {
				return
			}
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

	reqCtx, reqCancel := context.WithTimeout(s.serverCtx, 20*time.Second)
	defer reqCancel()
	req, err := http.NewRequestWithContext(reqCtx, reqMethod, reqURL, nil)
	if err != nil {
		s.logger.Warn("❌ [主动] 创建请求失败: %v", err)
		return
	}

	req.Header = reqHeaders.Clone()
	req.Host = reqHost

	resp, err := s.retryClient.Do(req)
	if err != nil {
		if s.serverCtx.Err() != nil {
			return
		}
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
	if err != nil || len(body) == 0 {
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

	overallCtx, overallCancel := context.WithTimeout(s.serverCtx, 60*time.Second)
	defer overallCancel()

	const maxAttempts = 30
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(overallCtx, "GET", fullURL, nil)
		if err != nil {
			return
		}
		req.Header = reqHeaders.Clone()
		req.Host = reqHost

		resp, err := s.retryClient.Do(req)
		if err != nil {
			if s.serverCtx.Err() != nil {
				return
			}
			if attempt < maxAttempts {
				time.Sleep(playbackProbeRetryInterval(attempt))
			}
			continue
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			if attempt < maxAttempts {
				time.Sleep(playbackProbeRetryInterval(attempt))
			}
			continue
		}

		body, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
		resp.Body.Close()
		if err != nil || len(body) == 0 {
			return
		}

		_, _ = s.playbackHandler.Handle(resp, body)
		return
	}
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

	overallCtx, overallCancel := context.WithTimeout(s.serverCtx, 60*time.Second)
	defer overallCancel()

	const maxAttempts = 18
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(overallCtx, "GET", fullURL, nil)
		if err != nil {
			return false
		}
		req.Header = reqHeaders.Clone()
		req.Host = reqHost

		resp, err := s.retryClient.Do(req)
		if err != nil {
			if s.serverCtx.Err() != nil {
				return false
			}
			if attempt < maxAttempts {
				time.Sleep(mediaSourceMissRetryInterval(attempt))
			}
			continue
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			if attempt < maxAttempts {
				time.Sleep(mediaSourceMissRetryInterval(attempt))
			}
			continue
		}

		body, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
		resp.Body.Close()
		if err != nil || len(body) == 0 {
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
			s.streamHandler.WaitForURLResolution(cachedMediaSourceID, itemID, 5*time.Second)
			return true
		}

		if attempt < maxAttempts {
			time.Sleep(mediaSourceMissRetryInterval(attempt))
		}
	}
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
		return
	}
	if !config.Global.GetEnablePreload() {
		return
	}
	if originalReq == nil {
		return
	}
	if originalReq.Header.Get("X-Fnysfd-Next-Prefetch") == "1" {
		return
	}
	if s.shouldSkipPrefetch(buildPrefetchKey("next", itemID, ""), 20, "下一集预取") {
		return
	}
	if userID == "" {
		userID = originalReq.URL.Query().Get("UserId")
	}

	s.proxyMu.RLock()
	targetURL := s.targetURL
	s.proxyMu.RUnlock()

	reqHeaders := originalReq.Header.Clone()
	reqHeaders.Del("Accept-Encoding")
	reqHeaders.Set("Accept", "application/json")
	reqHost := targetURL.Host

	overallCtx, overallCancel := context.WithTimeout(s.serverCtx, 60*time.Second)
	defer overallCancel()

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

	req, err := http.NewRequestWithContext(overallCtx, "GET", detailURL, nil)
	if err != nil {
		return
	}
	req.Header = reqHeaders.Clone()
	req.Host = reqHost

	resp, err := s.retryClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			resp.Body.Close()
		}
		return
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	resp.Body.Close()
	if err != nil || len(body) == 0 {
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
		return
	}
	if detail.Type != "" && detail.Type != "Episode" {
		return
	}
	if detail.ParentID == "" && detail.SeasonID != "" {
		detail.ParentID = detail.SeasonID
	}
	if detail.ParentID == "" {
		return
	}

	listQuery := url.Values{}
	listQuery.Set("ParentId", detail.ParentID)
	listQuery.Set("IncludeItemTypes", "Episode")
	listQuery.Set("SortBy", "IndexNumber")
	listQuery.Set("SortOrder", "Ascending")
	listQuery.Set("Fields", "BasicSyncInfo,MediaSources")
	listQuery.Set("Limit", "200")
	if userID != "" {
		listQuery.Set("UserId", userID)
	}

	var listResp struct {
		Items []struct {
			ID          string `json:"Id"`
			Name        string `json:"Name"`
			IndexNumber int    `json:"IndexNumber"`
		} `json:"Items"`
	}

	listURL := targetURL.Scheme + "://" + targetURL.Host + "/emby/Items?" + listQuery.Encode()
	req2, err := http.NewRequestWithContext(overallCtx, "GET", listURL, nil)
	if err != nil {
		return
	}
	req2.Header = reqHeaders.Clone()
	req2.Host = reqHost

	resp2, err := s.retryClient.Do(req2)
	if err != nil || resp2.StatusCode != http.StatusOK {
		if resp2 != nil {
			resp2.Body.Close()
		}
		return
	}
	body2, err := io.ReadAll(io.LimitReader(resp2.Body, 5*1024*1024))
	resp2.Body.Close()
	if err != nil || len(body2) == 0 {
		return
	}
	if err := json.Unmarshal(body2, &listResp); err != nil {
		return
	}

	curPos := -1
	for i, ep := range listResp.Items {
		if ep.ID == itemID {
			curPos = i
			break
		}
	}

	prefetched := 0
	for i, ep := range listResp.Items {
		if ep.ID == "" || ep.ID == itemID {
			continue
		}
		if detail.IndexNumber > 0 && ep.IndexNumber > 0 {
			if ep.IndexNumber <= detail.IndexNumber {
				continue
			}
		} else if curPos >= 0 && i <= curPos {
			continue
		}
		if _, found := s.cache.GetByItemID(ep.ID); found {
			continue
		}

		nextReq := originalReq.Clone(context.Background())
		nextReq.Header = originalReq.Header.Clone()
		nextReq.Header.Set("X-Fnysfd-Next-Prefetch", "1")
		go s.prefetchForDetailPage(nextReq, ep.ID, userID)
		prefetched++
		if prefetched >= 2 {
			break
		}
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

func (s *Server) markPrefetchDone(key string) {
	if key == "" {
		return
	}
	s.prefetchMu.Lock()
	defer s.prefetchMu.Unlock()
	s.prefetchRecent[key] = time.Now().Unix()
}

func buildPrefetchKey(prefix, itemID, mediaSourceID string) string {
	return prefix + ":" + itemID + "|" + mediaSourceID
}

func extractItemIDFromPath(path string) string {
	return util.ExtractItemIDFromPath(path)
}

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

func (s *Server) isPlaybackInfoRequest(req *http.Request) bool {
	if req.Method != "POST" && req.Method != "GET" {
		return false
	}
	path := req.URL.Path
	pathLower := strings.ToLower(path)
	playbackPatterns := []string{"/playbackinfo", "/playback-info", "/playback_info"}
	for _, pattern := range playbackPatterns {
		if strings.Contains(pathLower, pattern) {
			return true
		}
	}
	return false
}

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

func (s *Server) errorHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		safeW := &safeResponseWriter{ResponseWriter: w, written: false}
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
			w.Write([]byte(`{"error":"forbidden"}`))
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
		stats["cache"] = s.cache.GetStats()
	}
	if s.streamHandler != nil {
		stats["stream"] = s.streamHandler.GetStats()
	}
	if s.libraryScanner != nil {
		stats["libraryScan"] = s.libraryScanner.GetStatus()
	}
	if s.doubanProvider != nil { // ✅ 新增
		stats["douban"] = s.doubanProvider.GetStats()
	}

	jsonBytes, _ := json.Marshal(stats)
	w.Write(jsonBytes)
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte(`{"error":"method not allowed"}`))
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	health := map[string]string{"status": "ok", "uptime": time.Since(s.startTime).String()}
	jsonBytes, _ := json.Marshal(health)
	w.Write(jsonBytes)
}

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
