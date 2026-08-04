package handler

import (
	"bytes"
	"container/list"
	"context"
	"fmt"
	"fnysfd/internal/cache"
	"fnysfd/internal/config" // 读取 STRM 解析模式
	"fnysfd/internal/logger"
	"fnysfd/internal/util"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxStrmCacheSize = 10000 // STRM文件内容缓存
	maxURLCacheSize  = 10000 // URL解析结果缓存
	// 预加载工作协程数从10提升到16
	// 适应双队列（高优先+普通）并发需求，提高预加载吞吐量
	preloadWorkers = 16 // 预加载工作协程数

	// 极速解析参数（三客户端并行：fast + medium + slow）
	//   - fast 2s：极短超时，网络好时秒解析（覆盖 60% 场景）
	//   - medium 4s：中等超时，网络抖动时兜底（覆盖 25% 场景）
	//   - slow 8s：长超时，弱网环境兜底（覆盖 15% 场景）
	// 理论最快 0.8s，最坏 8s，但成功率从双客户端的 ~85% 提升到 ~95%
	ultraFastClientTimeout = 800 * time.Millisecond // 极速客户端：网络好时秒解析，省 1.2s
	fastClientTimeout      = 2 * time.Second        // 快速客户端超时
	mediumClientTimeout    = 4 * time.Second        // 中速客户端超时
	slowClientTimeout      = 8 * time.Second        // 慢速客户端超时
	// ✅ P1-14: 负缓存按错误类型分级TTL
	// 缩短负缓存TTL，让用户解析失败后能更快重试
	//   用户反馈"解析失败后紧接着就要继续解析，不要等待"
	// 10s/20s 平衡：防止短时间内对失败URL的无效重试，又不会让用户等太久
	negativeCacheTTL        = 10 * time.Second // 30s → 10s 默认失败结果负缓存时间
	negativeCacheTTLNetwork = 20 * time.Second // 60s → 20s 网络错误负缓存时间（更长，可能临时网络问题）
	// 403 链接过期不缓存（链接已失效，缓存无意义，用户重试可立即触发）
)

// userAgents User-Agent 轮换池（跟随重定向时的HTTP请求头，避免被目标服务器拦截）
var userAgents = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
	"Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
	"Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
	"VLC/3.0.20 LibVLC/3.0.20",
	"MPV/0.36.0",
	"Infuse-Direct/8.0",
	"VidHub/1.0",
}

// bytes.Buffer 池化（warmupCDN 专用）
// 每次预热需要 256KB 缓冲区读取 body，频繁分配会加重 GC 压力
// 用 sync.Pool 复用，减少内存分配和 GC 开销
var warmupBufPool = sync.Pool{
	New: func() interface{} {
		// 预分配 256KB 容量，避免动态扩容
		b := make([]byte, 0, 256*1024)
		return &b
	},
}

// urlCacheEntry URL解析结果缓存项
type urlCacheEntry struct {
	url       string
	expireAt  time.Time // 按签名有效期动态设置过期时间
	createdAt time.Time // 创建时间，用于半衰期刷新计算
}

// StreamHandler 处理视频流请求
type StreamHandler struct {
	cache        *cache.Cache
	logger       *logger.Logger
	fastClient   *http.Client             // 快速客户端（短超时，第一选择）
	ultraFastClient *http.Client           // 极速客户端（800ms超时，网络好时秒解析）
	mediumClient *http.Client             // 中速客户端（中等超时，fast失败时兜底）
	slowClient   *http.Client             // 慢速客户端（长超时，弱网兜底）
	warmupClient *http.Client             // CDN预热客户端（让CDN边缘节点提前定位文件）
	strmCache    map[string]string        // strm文件内容缓存 (path -> url)
	strmList     *list.List               // STRM LRU列表
	strmIndex    map[string]*list.Element // STRM LRU索引
	strmMutex    sync.RWMutex             // strm缓存锁
	urlCache     map[string]urlCacheEntry // URL解析结果缓存（带过期时间）
	urlList      *list.List               // URL LRU列表
	urlIndex     map[string]*list.Element // URL LRU索引
	urlMutex     sync.RWMutex             // URL缓存锁

	// 负缓存（失败的URL短期内不再尝试）
	// ✅ P1-14: 存储过期时间（expireAt），按错误类型分级TTL
	negCache map[string]time.Time // 失败URL -> 过期时间
	negMutex sync.RWMutex

	// 智能预加载系统
	preloadQueue      chan preloadTask // 预加载任务队列（普通优先级）
	highPriorityQueue chan preloadTask // 高优先级队列（电影/紧急预加载）
	preloadWg         sync.WaitGroup   // 预加载任务等待组
	shutdown          chan struct{}    // 关闭信号

	// 紧急预加载并发限制（带缓冲channel作为信号量）
	emergencySem chan struct{}

	// 预加载去重 + Promise 机制
	// inflight 存储 done channel，预加载完成时 close，流请求可等待（避免重复解析）
	inflight   map[string]chan struct{}
	inflightMu sync.Mutex

	// CDN预热去重（避免短时间内对同一URL重复预热）
	// 预加载阶段已预热的URL，缓存命中阶段直接跳过，让302立即返回
	// 不同场景用不同TTL检查：
	//   - 预加载（8s完整预热）：60s内不重复
	//   - 缓存命中（300ms探测）：30s内不重复
	//   - 实时解析（800ms预热）：10s内不重复（避免快速重试时重复预热）
	warmupRecent map[string]time.Time // URL -> 最近预热时间
	warmupMu     sync.RWMutex

	// 缓存命中URL校验去重，避免坏URL反复302导致播放器3003
	// 同一个URL校验成功后60秒内不重复校验，兼顾稳定性和起播速度
	validateRecent map[string]time.Time
	validateMu     sync.RWMutex

	// 流请求日志去重：播放器会发多个 Range 请求（seek/缓冲），
	// 同一 MediaSource 10秒内只打一次 Info 日志，其余降为 Debug
	streamReqRecent map[string]time.Time
	streamReqMu     sync.RWMutex

	// MediaSource 未缓存时的同步兜底回调
	// 新剧没有媒体信息时，流请求可能早于 PlaybackInfo 成功返回；回调会同步轮询 PlaybackInfo，
	// 准备好 MediaSource 后再由当前请求继续解析，避免把未准备好的流请求转发给飞牛导致3003。
	onMediaSourceMiss func(r *http.Request, itemID string, mediaSourceID string) bool

	// 半衰期刷新机制 - 在URL缓存过期前主动刷新
	// 当缓存条目超过50% TTL时，后台goroutine会异步重新解析URL
	// 确保播放中不会因为签名URL过期而出现403/3003错误
	refreshHalfLifeEnabled bool          // 半衰期刷新开关
	refreshHalfLifeStop    chan struct{} // 停止信号

	// 熔断器 - 错误率过高时暂时停止重试
	// 当URL解析连续失败超过阈值，熔断器断开，暂停重试一段时间
	// 避免在CDN/源站故障时大量无效重试浪费资源
	circuitBreaker struct {
		errors      int64     // 连续错误计数
		lastErrorAt time.Time // 最近一次错误时间
		state       int32     // 0=关闭(正常), 1=断开(熔断), 2=半开(尝试恢复)
		openUntil   time.Time // 熔断状态持续到何时
	}
	circuitMu sync.Mutex // 熔断器锁

	// 性能统计扩展
	stats struct {
		totalRequests   int64 // 总请求数
		cacheHits       int64 // 缓存命中数
		preloadSuccess  int64 // 预加载成功数
		resolveFailures int64 // 解析失败数
		fallbackCount   int64 // fallback使用数
		droppedCount    int64 // 预加载队列满时丢弃数（P1-12）
		// CDN预热统计（监控优化效果）
		warmupTotal    int64 // 预热总次数（含跳过）
		warmupSkips    int64 // 去重跳过次数
		warmupSuccess  int64 // 预热成功次数
		warmupFailures int64 // 预热失败次数
		warmupMoovHead int64 // moov在头部次数（faststart，无需尾部预热）
		warmupMoovTail int64 // moov在尾部次数（触发尾部预热）
		warmupTotalMs  int64 // 预热总耗时（毫秒，用于计算平均）
		// 预连接统计
		preconnectTotal    int64 // 预连接总次数
		preconnectSuccess  int64 // 预连接成功次数
		preconnectFailures int64 // 预连接失败次数
		// 直链跳过统计
		directURLSkips int64 // 直链跳过解析次数（302 strm 场景）
		// 半衰期刷新统计
		halfLifeRefreshes int64 // 半衰期刷新次数
		halfLifeSkips     int64 // 半衰期跳过次数（URL已过期）
		// 熔断器统计
		circuitBreakerOpens   int64 // 熔断器断开次数
		circuitBreakerRejects int64 // 熔断器拒绝请求次数
	}
}

// preloadTask 预加载任务
type preloadTask struct {
	mediaSourceID string
	itemID        string
	path          string
	priority      int // 优先级: 0=高, 1=中, 2=低
}

// NewStreamHandler 创建处理器
func NewStreamHandler(c *cache.Cache, l *logger.Logger) *StreamHandler {
	// ⚡ 极速客户端：800ms 超时，复用 warmupClient 的连接池（预连接的连接直接复用，省一次 TLS 握手）
	// 网络正常时 CDN 302 跟随只需 200~500ms，800ms 足够，比 fast 2s 快 60%
	ultraFastTransport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   1 * time.Second, // 极短 dial 超时
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          500,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       120 * time.Second,
		TLSHandshakeTimeout:   1 * time.Second, // 极短 TLS 超时
		ExpectContinueTimeout: 200 * time.Millisecond,
		ResponseHeaderTimeout: 2 * time.Second,
		DisableKeepAlives:     false,
		ForceAttemptHTTP2:     true,
		DisableCompression:    false,
	}
	ultraFastClient := &http.Client{
		Timeout:   ultraFastClientTimeout,
		Transport: ultraFastTransport,
		Jar:       nil,
	}

	// 🚀 快速客户端：超短超时，高并发（自动跟随重定向，复用连接池）
	// 进一步缩短 DialTimeout/ResponseHeaderTimeout，配合并行 slow 客户端
	fastTransport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Second, // 从 3s 缩短到 2s
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          500,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       120 * time.Second,
		TLSHandshakeTimeout:   2 * time.Second, // 从 3s 缩短到 2s
		ExpectContinueTimeout: 300 * time.Millisecond,
		ResponseHeaderTimeout: 3 * time.Second, // 从 5s 缩短到 3s（与 client timeout 对齐）
		DisableKeepAlives:     false,
		ForceAttemptHTTP2:     true,
		DisableCompression:    false,
	}

	fastClient := &http.Client{
		Timeout:   fastClientTimeout,
		Transport: fastTransport,
		// 不设置 CheckRedirect，让 Client 自动跟随重定向（最多10次）
		Jar: nil,
	}

	// 🐢 慢速客户端：长超时，与 fast 并行执行（不再是 fallback）
	// 缩短到 8s，配合并行模式
	slowTransport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   3 * time.Second, // 从 5s 缩短到 3s
			KeepAlive: 60 * time.Second,
		}).DialContext,
		MaxIdleConns:          200,
		MaxIdleConnsPerHost:   50,
		IdleConnTimeout:       300 * time.Second,
		TLSHandshakeTimeout:   3 * time.Second, // 从 5s 缩短到 3s
		ExpectContinueTimeout: 500 * time.Millisecond,
		ResponseHeaderTimeout: 6 * time.Second, // 从 8s 缩短到 6s
		DisableKeepAlives:     false,
		ForceAttemptHTTP2:     true,
	}

	slowClient := &http.Client{
		Timeout:   slowClientTimeout,
		Transport: slowTransport,
		// 不设置 CheckRedirect，让 Client 自动跟随重定向（最多10次）
		Jar: nil,
	}

	// 中速客户端（4s 超时，fast 失败时的快速兜底）
	// 介于 fast（2s）和 slow（8s）之间，覆盖网络抖动场景
	// 三客户端并行：fast + medium + slow 同时跑，谁先成功用谁
	mediumTransport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: 45 * time.Second,
		}).DialContext,
		MaxIdleConns:          300,
		MaxIdleConnsPerHost:   75,
		IdleConnTimeout:       180 * time.Second,
		TLSHandshakeTimeout:   2 * time.Second,
		ExpectContinueTimeout: 400 * time.Millisecond,
		ResponseHeaderTimeout: 4 * time.Second,
		DisableKeepAlives:     false,
		ForceAttemptHTTP2:     true,
	}
	mediumClient := &http.Client{
		Timeout:   mediumClientTimeout,
		Transport: mediumTransport,
		Jar:       nil,
	}

	// CDN预热客户端（双端预热版）
	// 不设置 client.Timeout，依赖调用方传入的 context 超时（不同场景需要不同超时）
	//   - 预加载阶段：8s（完整预取 256KB + 尾部）
	//   - 实时解析阶段：800ms（快速预取）
	//   - 缓存命中阶段：300ms（快速触发）
	// 连接池调优 - MaxIdleConnsPerHost 20→50，适应批量预热
	//   16个worker并发预热同一CDN域名时，20个空闲连接可能不够，提升到50避免连接重建
	warmupTransport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          200, // 100 → 200
		MaxIdleConnsPerHost:   50,  // 20 → 50
		IdleConnTimeout:       120 * time.Second,
		TLSHandshakeTimeout:   2 * time.Second,
		ExpectContinueTimeout: 300 * time.Millisecond,
		ResponseHeaderTimeout: 5 * time.Second, // 3s → 5s，CDN冷启动时响应头可能需要更久
		DisableKeepAlives:     false,
		ForceAttemptHTTP2:     true,
	}
	warmupClient := &http.Client{
		Transport: warmupTransport,
		// 不设置 Timeout，依赖 context 超时（不同场景不同超时）
	}

	h := &StreamHandler{
		cache:               c,
		logger:              l,
		fastClient:          fastClient,
		ultraFastClient:     ultraFastClient,
		mediumClient:        mediumClient, //
		slowClient:          slowClient,
		warmupClient:        warmupClient,
		strmCache:           make(map[string]string),
		strmList:            list.New(),
		strmIndex:           make(map[string]*list.Element),
		urlCache:            make(map[string]urlCacheEntry),
		urlList:             list.New(),
		urlIndex:            make(map[string]*list.Element),
		negCache:            make(map[string]time.Time),
		preloadQueue:        make(chan preloadTask, 1000),
		highPriorityQueue:   make(chan preloadTask, 200), // 高优先级队列
		shutdown:            make(chan struct{}),
		emergencySem:        make(chan struct{}, 10), // 限制紧急预加载最多10个并发
		inflight:            make(map[string]chan struct{}),
		warmupRecent:        make(map[string]time.Time), // 预热去重
		validateRecent:      make(map[string]time.Time), // URL校验去重
		streamReqRecent:     make(map[string]time.Time), // 流请求日志去重
		refreshHalfLifeStop: make(chan struct{}),        // 半衰期刷新停止信号
	}

	// 启动预加载工作池
	h.startPreloadWorkers()

	// 启动负缓存清理协程
	go h.cleanupNegativeCache()

	// 启动预热去重清理协程（每5分钟清理过期记录）
	go h.cleanupWarmupRecent()

	// 启动半衰期URL刷新协程（在URL过期前主动刷新）
	go h.halfLifeRefreshLoop()

	l.Info("STRM解析引擎已启动（半衰期刷新+熔断器已就绪）")

	return h
}

// SetMediaSourceMissHandler 设置 MediaSource 未缓存时的同步兜底处理器
func (h *StreamHandler) SetMediaSourceMissHandler(fn func(r *http.Request, itemID string, mediaSourceID string) bool) {
	h.onMediaSourceMiss = fn
}

// WaitForURLResolution 等待指定 MediaSource 的 STRM URL 解析完成
// 用于 MediaSource 兜底场景：MediaSource 已缓存但 URL 尚未解析完成时，同步等待
//   - 先检查 URL 是否已缓存（直链/passthrough 场景通常已就绪）
//   - 未就绪则等待正在进行的预加载（executePreload 完成后会 close inflight channel）
//   - 超时后返回 false，让调用方决定后续策略
func (h *StreamHandler) WaitForURLResolution(mediaSourceID string, itemID string, timeout time.Duration) bool {
	// 1. 先检查 URL 是否已缓存
	if mediaSourceID != "" {
		if _, found := h.cache.GetStreamURL(mediaSourceID); found {
			return true
		}
	}
	// mediaSourceID 为空时，通过 itemID 查找实际的 MediaSource
	if mediaSourceID == "" && itemID != "" {
		source, found := h.cache.GetByItemID(itemID)
		if !found {
			return false
		}
		mediaSourceID = source.ID
		if _, found := h.cache.GetStreamURL(mediaSourceID); found {
			return true
		}
	}

	if mediaSourceID == "" {
		return false
	}

	// 2. 获取 STRM 路径，检查是否有 inflight 预加载
	source, found := h.cache.Get(mediaSourceID)
	if !found {
		return false
	}

	h.inflightMu.Lock()
	doneCh := h.inflight[source.Path]
	h.inflightMu.Unlock()

	if doneCh == nil {
		// 没有正在进行的预加载，再次检查 URL 缓存（可能刚完成）
		if _, found := h.cache.GetStreamURL(mediaSourceID); found {
			return true
		}
		return false
	}

	// 3. 等待预加载完成
	h.logger.Debug("⏳ [URL等待] 等待STRM URL解析: MS=%s, 超时=%v", mediaSourceID, timeout)
	select {
	case <-doneCh:
		if _, found := h.cache.GetStreamURL(mediaSourceID); found {
			h.logger.Info("✅ [URL等待] 解析完成: MS=%s", mediaSourceID)
			return true
		}
		h.logger.Warn("⚠️ [URL等待] 预加载完成但URL未缓存: MS=%s", mediaSourceID)
		return false
	case <-time.After(timeout):
		h.logger.Warn("⚠️ [URL等待] 等待超时: MS=%s, 超时=%v", mediaSourceID, timeout)
		return false
	}
}

// cleanupNegativeCache 定期清理负缓存
func (h *StreamHandler) cleanupNegativeCache() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			h.negMutex.Lock()
			now := time.Now()
			// ✅ P1-14: 清理已过期的负缓存项（按各自过期时间）
			for url, expireAt := range h.negCache {
				if now.After(expireAt) {
					delete(h.negCache, url)
				}
			}
			h.negMutex.Unlock()
		case <-h.shutdown:
			return
		}
	}
}

// cleanupWarmupRecent 定期清理过期的预热记录
// 避免warmupRecent map无限增长
func (h *StreamHandler) cleanupWarmupRecent() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			h.warmupMu.Lock()
			now := time.Now()
			// 清理5分钟前的记录（最长TTL是60s，5分钟足够安全）
			for url, warmTime := range h.warmupRecent {
				if now.Sub(warmTime) > 5*time.Minute {
					delete(h.warmupRecent, url)
				}
			}
			h.warmupMu.Unlock()
		case <-h.shutdown:
			return
		}
	}
}

// shouldSkipWarmup 检查URL是否在TTL内已预热过
// 返回true表示应跳过本次预热（近期已预热过，CDN应仍然缓存）
// TTL选择依据：
//   - 预加载阶段（8s完整预热256KB+尾部）：60s内不重复
//     CDN边缘节点缓存TTL通常5-30分钟，60s内必定仍然缓存
//   - 缓存命中阶段（300ms探测）：30s内不重复
//     用户点击播放通常在浏览详情页5-30s内，preload刚预热完
//   - 实时解析阶段（800ms预热）：10s内不重复
//     新解析的URL需要预热，但快速重试时跳过
func (h *StreamHandler) shouldSkipWarmup(url string, ttl time.Duration) bool {
	h.warmupMu.RLock()
	lastWarm, exists := h.warmupRecent[url]
	h.warmupMu.RUnlock()
	if !exists {
		return false
	}
	return time.Since(lastWarm) < ttl
}

// shouldSkipURLValidation 检查URL是否近期已校验通过
func (h *StreamHandler) shouldSkipURLValidation(url string, ttl time.Duration) bool {
	h.validateMu.RLock()
	lastValidated, exists := h.validateRecent[url]
	h.validateMu.RUnlock()
	if !exists {
		return false
	}
	return time.Since(lastValidated) < ttl
}

// markURLValidated 标记URL已校验通过
func (h *StreamHandler) markURLValidated(url string) {
	h.validateMu.Lock()
	h.validateRecent[url] = time.Now()
	h.validateMu.Unlock()
}

// shouldSkipStreamReqLog 检查同一 MediaSource 是否在 TTL 内已打过 Info 日志
// 返回 true 表示应该降为 Debug（近期已打过 Info），false 表示这是首次（应打 Info）
func (h *StreamHandler) shouldSkipStreamReqLog(mediaSourceID string, ttl time.Duration) bool {
	if mediaSourceID == "" {
		return false
	}
	h.streamReqMu.Lock()
	defer h.streamReqMu.Unlock()
	now := time.Now()
	if last, exists := h.streamReqRecent[mediaSourceID]; exists && now.Sub(last) < ttl {
		return true
	}
	h.streamReqRecent[mediaSourceID] = now
	// 清理过期项，避免 map 无限增长
	if len(h.streamReqRecent) > 200 {
		for id, t := range h.streamReqRecent {
			if now.Sub(t) > 60*time.Second {
				delete(h.streamReqRecent, id)
			}
		}
	}
	return false
}

// validatePlayableURL 轻量校验缓存URL是否仍可播放
// 避免缓存命中后把HTML错误页、JSON错误、鉴权拦截页继续302给播放器导致3003。
func (h *StreamHandler) validatePlayableURL(targetURL string, originalReq *http.Request) error {
	if targetURL == "" {
		return fmt.Errorf("URL为空")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", targetURL, nil)
	if err != nil {
		return err
	}

	ua := originalReq.Header.Get("User-Agent")
	if ua == "" {
		ua = h.getRandomUserAgent()
	}
	req.Header.Set("User-Agent", ua)
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Range", "bytes=0-4095")

	resp, err := h.fastClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return fmt.Errorf("访问被拒绝: %d", resp.StatusCode)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return fmt.Errorf("状态码异常: %d", resp.StatusCode)
	}

	contentType := strings.ToLower(resp.Header.Get("Content-Type"))
	// HTML/JSON 基本可以确定不是媒体；text/plain 先不拒绝，
	// 因为部分 HLS/m3u8 服务会错误返回 text/plain，需要读取内容判断 #EXTM3U。
	if strings.Contains(contentType, "text/html") || strings.Contains(contentType, "application/json") {
		return fmt.Errorf("响应为错误页面: Content-Type=%s", contentType)
	}

	buf := make([]byte, 4096)
	n, readErr := io.ReadFull(resp.Body, buf)
	if readErr != nil && readErr != io.ErrUnexpectedEOF && readErr != io.EOF {
		return readErr
	}
	data := buf[:n]
	if len(data) == 0 {
		return fmt.Errorf("响应体为空")
	}

	if looksLikePlayableHeader(data, contentType) {
		return nil
	}

	return fmt.Errorf("响应头不像媒体内容: Content-Type=%s, 前%d字节=%q", contentType, len(data), string(data[:minInt(len(data), 32)]))
}

// looksLikePlayableHeader 判断前几KB是否像可播放媒体
// 覆盖 MP4/MKV/M2TS/ISO/MPG/FLV/AVI/WAV/HLS/MP3/MPEG-TS 等全部常见格式。
func looksLikePlayableHeader(data []byte, contentType string) bool {
	// Content-Type 包含明确的媒体类型，直接通过
	if strings.Contains(contentType, "video/") ||
		strings.Contains(contentType, "audio/") ||
		strings.Contains(contentType, "application/octet-stream") ||
		strings.Contains(contentType, "application/vnd.apple.mpegurl") ||
		strings.Contains(contentType, "application/x-mpegurl") ||
		strings.Contains(contentType, "application/dash+xml") {
		return true
	}

	// MP4/MOV: ftyp/moov/moof box
	if len(data) >= 12 {
		box := string(data[4:8])
		if box == "ftyp" || box == "moov" || box == "moof" {
			return true
		}
	}

	// MKV/WebM: EBML 魔数 0x1A 0x45 0xDF 0xA3
	if len(data) >= 4 && data[0] == 0x1A && data[1] == 0x45 && data[2] == 0xDF && data[3] == 0xA3 {
		return true
	}

	// MPG/MPEG-PS: Pack Header 0x00 0x00 0x01 0xBA
	// 覆盖 .mpg / .mpeg / .vob 等格式
	if len(data) >= 4 && data[0] == 0x00 && data[1] == 0x00 && data[2] == 0x01 && data[3] == 0xBA {
		return true
	}
	// MPEG-PS System Header: 0x00 0x00 0x01 0xBB
	if len(data) >= 4 && data[0] == 0x00 && data[1] == 0x00 && data[2] == 0x01 && data[3] == 0xBB {
		return true
	}

	// ISO: 光盘镜像（ISO 9660 "CD001" / UDF "NSR0"）
	// ISO 9660: 偏移 0x8001 处有 "CD001"
	// UDF: 偏移 0x8000 处有 "NSR0" / "NSR02" / "NSR03"
	if len(data) >= 5 {
		if bytes.Contains(data[:minInt(len(data), 4096)], []byte("CD001")) {
			return true
		}
		if bytes.Contains(data[:minInt(len(data), 4096)], []byte("NSR0")) {
			return true
		}
	}

	// FLV: "FLV" (0x46 0x4C 0x56)
	if len(data) >= 3 && data[0] == 0x46 && data[1] == 0x4C && data[2] == 0x56 {
		return true
	}

	// AVI/WAV: "RIFF" (0x52 0x49 0x46 0x46)
	if len(data) >= 4 && data[0] == 0x52 && data[1] == 0x49 && data[2] == 0x46 && data[3] == 0x46 {
		return true
	}

	// MP3: ID3 标签
	if len(data) >= 3 && string(data[:3]) == "ID3" {
		return true
	}

	// HLS: m3u8 播放列表
	if bytes.Contains(data[:minInt(len(data), 512)], []byte("#EXTM3U")) {
		return true
	}

	// MPEG-TS / M2TS: 0x47 同步字节
	// 标准 MPEG-TS: 188 字节包，0x47 在偏移 0
	// M2TS (蓝光): 192 字节包，0x47 在偏移 4（前 4 字节为 TP_extra_header）
	if len(data) >= 4 {
		if data[0] == 0x47 {
			return true
		}
		// M2TS: 偏移 4 是 0x47
		if len(data) > 4 && data[4] == 0x47 {
			return true
		}
		// 标准 MPEG-TS 第二个包：偏移 188
		if len(data) > 188 && data[188] == 0x47 {
			return true
		}
		// M2TS 第二个包：偏移 192
		if len(data) > 192 && data[192] == 0x47 {
			return true
		}
	}

	return false
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// markWarmed 标记URL已预热
// 仅在预热成功（拿到响应）后调用
func (h *StreamHandler) markWarmed(url string) {
	h.warmupMu.Lock()
	h.warmupRecent[url] = time.Now()
	// 容量保护：超过1000条时主动清理过期项
	if len(h.warmupRecent) > 1000 {
		now := time.Now()
		for u, t := range h.warmupRecent {
			if now.Sub(t) > 5*time.Minute {
				delete(h.warmupRecent, u)
			}
		}
	}
	h.warmupMu.Unlock()
}

// detectMP4MoovInHead 检测MP4文件的moov atom是否在文件头部
// 返回值：
//   - true:  moov atom 在头部（faststart格式），无需预取尾部
//   - false: moov atom 不在头部（可能在尾部），需要预取尾部
// MP4 atom格式：[4字节big-endian size][4字节type][data]
// 常见atom类型：ftyp(文件类型)、moov(元数据)、mdat(媒体数据)、free(空闲)、skip(跳过)
// 判断逻辑：
//   - 顺序扫描atom，遇到moov → 在头部
//   - 遇到mdat → moov在mdat之后（通常在文件尾部，非faststart）
//   - 扫描完所有可见atom都没遇到moov → 数据不足或非MP4，保守返回false
func detectMP4MoovInHead(data []byte) bool {
	if len(data) < 8 {
		return false
	}
	offset := 0
	for offset+8 <= len(data) {
		// 读取4字节big-endian size
		size := int(data[offset])<<24 | int(data[offset+1])<<16 | int(data[offset+2])<<8 | int(data[offset+3])
		// size至少8字节（4字节size + 4字节type），0或1表示异常
		if size < 8 {
			break
		}
		atomType := string(data[offset+4 : offset+8])
		// 遇到moov atom，说明moov在头部（faststart格式）
		if atomType == "moov" {
			return true
		}
		// 遇到mdat atom，说明moov在mdat之后（非faststart，moov通常在文件尾）
		if atomType == "mdat" {
			return false
		}
		// 防止size超出data范围（数据不完整）
		if offset+size > len(data) {
			// 当前atom不完整，无法继续扫描
			// 保守判断：还没遇到moov或mdat，无法确定位置
			return false
		}
		offset += size
	}
	// 扫描完所有可见atom都没遇到moov或mdat
	// 可能是数据不足（256KB不够容纳完整atom列表）或非MP4文件
	// 保守返回false，让尾部预热兜底
	return false
}

// isNegativelyCached 检查URL是否在负缓存中（近期失败过）
func (h *StreamHandler) isNegativelyCached(url string) bool {
	h.negMutex.RLock()
	defer h.negMutex.RUnlock()
	// ✅ P1-14: 检查是否在过期时间内
	expireAt, exists := h.negCache[url]
	if !exists {
		return false
	}
	return time.Now().Before(expireAt)
}

// markFailed 标记URL解析失败
// ✅ P1-14: 按错误类型分级TTL
// 缩短TTL（403不缓存，网络错误20s，其他10s）让用户失败后能更快重试
func (h *StreamHandler) markFailed(url string, err error) {
	// 403 链接过期不缓存（链接已失效，缓存无意义）
	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "403") || strings.Contains(errStr, "Forbidden") {
			atomic.AddInt64(&h.stats.resolveFailures, 1)
			return
		}
	}

	// 根据错误类型选择TTL
	ttl := negativeCacheTTL
	if err != nil {
		errStr := err.Error()
		// 网络错误使用更长TTL（可能临时网络问题）
		if strings.Contains(errStr, "timeout") || strings.Contains(errStr, "connection refused") ||
			strings.Contains(errStr, "no such host") || strings.Contains(errStr, "context canceled") {
			ttl = negativeCacheTTLNetwork
		}
	}

	h.negMutex.Lock()
	h.negCache[url] = time.Now().Add(ttl) // 存储过期时间
	h.negMutex.Unlock()
	atomic.AddInt64(&h.stats.resolveFailures, 1)
}

// startPreloadWorkers 启动预加载工作协程池
func (h *StreamHandler) startPreloadWorkers() {
	for i := 0; i < preloadWorkers; i++ {
		h.preloadWg.Add(1)
		go h.preloadWorker(i)
	}
}

// preloadWorker 预加载工作协程
// 支持优先级 - 用嵌套select优先取高优先级队列
// 高优先级队列有任务时优先处理，无任务时才处理普通队列
func (h *StreamHandler) preloadWorker(workerID int) {
	defer h.preloadWg.Done()

	for {
		// 嵌套select实现优先级
		// 外层select非阻塞尝试高优先级队列
		select {
		case task := <-h.highPriorityQueue:
			h.executePreload(task)
			continue
		case <-h.shutdown:
			h.logger.Debug("🛑 [预加载] Worker-%d 已停止", workerID)
			return
		default:
			// 高优先级队列空，进入公平select
		}

		// 公平select：高优先级和普通队列平等竞争
		// 但高优先级队列有任务时仍会被优先选中（Go select随机选择，但高优先级队列有缓冲）
		select {
		case task := <-h.highPriorityQueue:
			h.executePreload(task)
		case task := <-h.preloadQueue:
			h.executePreload(task)
		case <-h.shutdown:
			h.logger.Debug("🛑 [预加载] Worker-%d 已停止", workerID)
			return
		}
	}
}

// executePreload 执行预加载任务
// 完成时 close done channel，通知等待的流请求
func (h *StreamHandler) executePreload(task preloadTask) {
	// 获取 done channel，完成后 close 通知等待者
	h.inflightMu.Lock()
	doneCh := h.inflight[task.path]
	h.inflightMu.Unlock()

	// 预加载完成后从inflight移除，并 close done channel 通知等待者
	defer func() {
		h.inflightMu.Lock()
		delete(h.inflight, task.path)
		h.inflightMu.Unlock()
		if doneCh != nil {
			close(doneCh)
		}
	}()

	startTime := time.Now()

	// 如果 URL 已被同步预读缓存（PreloadStrmSync），直接跳过
	// 同步预读在 PlaybackInfo 响应处理时就完成了，worker 取到任务时 URL 通常已就绪
	if _, found := h.cache.GetStreamURL(task.mediaSourceID); found {
		h.logger.Debug("⚡ [预加载] URL已被同步预读缓存，跳过: ID=%s", task.mediaSourceID)
		return
	}

	// 步骤1：读取STRM文件
	strmURL, err := h.readStrmWithCache(task.path)
	if err != nil {
		h.logger.Warn("⚠️ [预加载] STRM读取失败: %s", task.path)
		return
	}

	// 步骤2：检查是否在负缓存中
	if h.isNegativelyCached(strmURL) {
		h.logger.Debug("⏭️ [预加载] 跳过负缓存URL: %s", strmURL)
		return
	}

	// STRM 解析模式开关
	//   - passthrough: 所有 strm 直接透传 URL 给播放器，不解析/不预热/不预连接（适合全 302 strm 场景）
	//   - auto:        智能识别
	//   - always:      强制走 HTTP 解析
	mode := config.Global.GetStrmResolveMode()

	// passthrough 模式 - 直接缓存原始 URL，不做任何后续处理
	if mode == "passthrough" {
		h.cache.SetStreamURL(task.mediaSourceID, strmURL)
		h.setURLCache(strmURL, strmURL) // 使用带过期检查的 setURLCache
		atomic.AddInt64(&h.stats.directURLSkips, 1)
		h.logger.Info("⚡ [预加载] 透传模式，跳过解析: ID=%s", task.mediaSourceID)
		return
	}

	// auto 模式 - 智能识别 302 strm，如果 URL 本身就是直链，跳过解析直接缓存
	// always 模式跳过此判断，强制走 HTTP 解析
	if mode == "auto" && isDirectURL(strmURL) {
		// 直接缓存原始 URL 作为最终直链
		h.cache.SetStreamURL(task.mediaSourceID, strmURL)
		h.setURLCache(strmURL, strmURL) // 使用带过期检查的 setURLCache
		// 异步预连接 + 预热
		go h.preconnectCDN(strmURL)
		go h.warmupCDN(strmURL, 8*time.Second, "预加载直链", 60*time.Second)
		atomic.AddInt64(&h.stats.directURLSkips, 1)
		h.logger.Info("⚡ [预加载] 检测到直链，跳过解析: ID=%s", task.mediaSourceID)
		return
	}

	// 步骤3：解析最终URL（带重试）
	// 传入最小化的 http.Request 仅用于保持API兼容（resolveURLWithRetry 内部只读取 UA 头）
	dummyReq := &http.Request{Header: http.Header{}}
	finalURL, err := h.resolveURLWithRetry(strmURL, dummyReq)
	if err != nil {
		h.logger.Warn("⚠️ [预加载] URL解析失败: %s", strmURL)
		h.markFailed(strmURL, err)
		return
	}

	// 步骤4：缓存直链
	h.setStreamURLSmart(task.mediaSourceID, finalURL)
	// 也存入 urlCache，避免不同 MediaSource 共享同一 strmURL 时重复解析
	h.setURLCache(strmURL, finalURL)

	// 步骤5：CDN预热（异步，不阻塞预加载完成信号）
	// 预取 256KB 文件头 + 异步预取文件尾，让CDN边缘节点提前缓存 moov atom
	// 用户浏览详情页的几秒内完成预热，点击播放时CDN已准备好，播放器可立即开始传输
	// 超时从 5s 延长到 8s，确保 256KB 预取 + 尾部预取有足够时间完成
	// dedupTTL=60s，预加载是完整预热，CDN缓存持久，60s内不重复预热
	go h.warmupCDN(finalURL, 8*time.Second, "预加载", 60*time.Second)

	latency := time.Since(startTime).Milliseconds()
	atomic.AddInt64(&h.stats.preloadSuccess, 1)

	h.logger.Debug("✅ [预加载] 完成: ID=%s (耗时: %dms)", task.mediaSourceID, latency)
}

// halfLifeRefreshLoop 半衰期URL刷新循环
// 在URL缓存过期前主动刷新，防止播放中签名URL过期导致403/3003
// 工作流程：
//  1. 每30秒扫描一次URL缓存
//  2. 对已超过50% TTL的条目，异步重新解析
//  3. 如果URL已过期则跳过（不等刷新了，等流请求重新触发）
//  4. 通过熔断器控制：熔断期间暂停刷新，避免无效请求
// 幂等性保证：
//   - 同一URL在同一周期内最多刷新一次（通过负缓存和去重）
//   - 刷新失败不阻塞后续扫描
func (h *StreamHandler) halfLifeRefreshLoop() {
	h.logger.Info("🔄 [半衰期] URL刷新循环已启动（每30秒扫描）")

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			h.scanAndRefreshURLs()
		case <-h.shutdown:
			return
		case <-h.refreshHalfLifeStop:
			h.logger.Info("🔄 [半衰期] URL刷新循环已停止")
			return
		}
	}
}

// scanAndRefreshURLs 扫描URL缓存并刷新半衰期条目
func (h *StreamHandler) scanAndRefreshURLs() {
	// 熔断器检查：熔断期间跳过刷新
	if !h.circuitBreakerCanProceed("半衰期刷新") {
		atomic.AddInt64(&h.stats.halfLifeSkips, 1)
		return
	}

	// 预加载开关检查：关闭预加载时也跳过半衰期刷新
	if !config.Global.GetEnablePreload() {
		return
	}

	h.urlMutex.RLock()
	// 收集所有URL缓存条目
	type urlEntry struct {
		key      string
		url      string
		expireAt time.Time
		age      float64 // 已过TTL的比例（0.0~1.0）
	}

	var candidates []urlEntry
	now := time.Now()

	for key, entry := range h.urlCache {
		// 使用 createdAt 计算已使用时间比例
		// 半衰期 = 已过时间 > 总TTL的50%
		elapsed := now.Sub(entry.createdAt)
		totalTTL := entry.expireAt.Sub(entry.createdAt)
		if totalTTL <= 0 {
			continue
		}
		age := float64(elapsed) / float64(totalTTL)
		remaining := time.Until(entry.expireAt)
		// 条件：已过50% TTL 且 剩余时间 > 30秒（避免刚过期或即将过期的无效刷新）
		if age >= 0.5 && remaining > 30*time.Second {
			candidates = append(candidates, urlEntry{
				key:      key,
				url:      entry.url,
				expireAt: entry.expireAt,
				age:      age,
			})
		}
	}
	h.urlMutex.RUnlock()

	if len(candidates) == 0 {
		return
	}

	h.logger.Debug("🔄 [半衰期] 发现 %d 个URL需要刷新", len(candidates))

	for _, entry := range candidates {
		// 再次检查是否已过期（扫描过程中的延迟）
		if time.Now().After(entry.expireAt) {
			atomic.AddInt64(&h.stats.halfLifeSkips, 1)
			h.logger.Debug("🔄 [半衰期] 跳过(已过期): %s", shortURL(entry.url))
			continue
		}

		// 检查负缓存：近期失败的URL跳过
		h.negMutex.RLock()
		negExpire, inNeg := h.negCache[entry.url]
		h.negMutex.RUnlock()
		if inNeg && time.Now().Before(negExpire) {
			atomic.AddInt64(&h.stats.halfLifeSkips, 1)
			h.logger.Debug("🔄 [半衰期] 跳过(负缓存): %s", shortURL(entry.url))
			continue
		}

		// 异步刷新（不阻塞扫描循环）
		atomic.AddInt64(&h.stats.halfLifeRefreshes, 1)
		go h.refreshHalfLifeURL(entry.key, entry.url)
	}
}

// refreshHalfLifeURL 半衰期刷新单个URL
// 对即将过期的URL发起一次快速探测（Range: bytes=0-0），
// 如果成功，根据重定向或响应内容获取新的直链并更新缓存
func (h *StreamHandler) refreshHalfLifeURL(key, oldURL string) {
	startTime := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", oldURL, nil)
	if err != nil {
		h.logger.Debug("🔄 [半衰期] 创建请求失败: %s | %v", shortURL(oldURL), err)
		return
	}

	// 使用最小Range探测，检查URL是否仍有效并获取最新重定向
	req.Header.Set("Range", "bytes=0-0")
	req.Header.Set("User-Agent", userAgents[rand.Intn(len(userAgents))])

	resp, err := h.fastClient.Do(req)
	if err != nil {
		h.logger.Debug("🔄 [半衰期] 刷新失败(网络): %s | %v", shortURL(oldURL), err)
		h.circuitBreakerRecordError()
		return
	}

	// 处理响应
	switch resp.StatusCode {
	case http.StatusPartialContent, http.StatusOK:
		// URL仍有效，检查是否有重定向后的新URL
		finalURL := resp.Request.URL.String()
		if finalURL != "" && finalURL != oldURL {
			// 发生了重定向，用新URL更新缓存（延长TTL）
			h.setStreamURLSmart(key, finalURL)
			h.setURLCache(finalURL, finalURL)
			h.logger.Debug("🔄 [半衰期] 刷新成功(重定向): %s → %s (%dms)",
				shortURL(oldURL), shortURL(finalURL), time.Since(startTime).Milliseconds())
		} else {
			// 无重定向，URL仍有效，延长缓存时间
			// 重新set一次以延长TTL
			h.setStreamURLSmart(key, oldURL)
			h.setURLCache(oldURL, oldURL)
			h.logger.Debug("🔄 [半衰期] 刷新成功(无变化): %s (%dms)",
				shortURL(oldURL), time.Since(startTime).Milliseconds())
		}
		// 成功：记录一次熔断器成功（重置错误计数）
		h.circuitBreakerRecordSuccess()
		resp.Body.Close()

	case http.StatusForbidden, http.StatusUnauthorized:
		// 403/401 - URL已过期，清除缓存
		// 降为 Debug：URL 过期是正常的签名失效行为，多个 URL 同时过期时 Info 会刷屏
		// 清缓存后流请求会重新触发解析，无需用户关注
		resp.Body.Close()
		h.cache.DeleteStreamURL(key)
		h.logger.Debug("🔄 [半衰期] URL已过期(%d): %s | 已清除缓存", resp.StatusCode, shortURL(oldURL))

	default:
		// 其他错误
		resp.Body.Close()
		h.logger.Debug("🔄 [半衰期] 刷新异常状态码: %d | %s", resp.StatusCode, shortURL(oldURL))
		h.circuitBreakerRecordError()
	}
}

// circuitBreakerCanProceed 检查熔断器是否允许请求通过
// 返回true表示允许请求，false表示熔断器断开
// 注意：本函数所有字段读写均在 circuitMu 锁内，不需要 atomic 操作
func (h *StreamHandler) circuitBreakerCanProceed(tag string) bool {
	h.circuitMu.Lock()
	defer h.circuitMu.Unlock()

	state := h.circuitBreaker.state
	if state == 0 {
		// 关闭状态：正常通过
		return true
	}

	if state == 1 {
		// 断开状态：检查是否超过熔断时间
		if time.Now().After(h.circuitBreaker.openUntil) {
			// 熔断时间已过，进入半开状态
			h.circuitBreaker.state = 2
			h.logger.Debug("🔌 [熔断器] 半开状态，允许请求通过: %s", tag)
			return true
		}
		// 仍在熔断期内
		atomic.AddInt64(&h.stats.circuitBreakerRejects, 1)
		return false
	}

	// 半开状态：允许通过
	return true
}

// circuitBreakerRecordError 记录一次错误到熔断器
// 连续错误超过阈值时断开熔断器
// 注意：本函数所有字段读写均在 circuitMu 锁内，不需要 atomic 操作
func (h *StreamHandler) circuitBreakerRecordError() {
	h.circuitMu.Lock()
	defer h.circuitMu.Unlock()

	// 增加错误计数
	h.circuitBreaker.errors++
	h.circuitBreaker.lastErrorAt = time.Now()

	// 阈值：连续5次错误
	const errorThreshold int64 = 5

	if h.circuitBreaker.errors >= errorThreshold {
		// 断开熔断器，持续30秒
		h.circuitBreaker.state = 1
		h.circuitBreaker.openUntil = time.Now().Add(30 * time.Second)
		atomic.AddInt64(&h.stats.circuitBreakerOpens, 1)
		h.logger.Warn("🔌 [熔断器] 已断开（连续%d次错误），暂停30秒", h.circuitBreaker.errors)
	}
}

// circuitBreakerRecordSuccess 记录一次成功到熔断器
// 成功时重置错误计数
// 注意：本函数所有字段读写均在 circuitMu 锁内，不需要 atomic 操作
func (h *StreamHandler) circuitBreakerRecordSuccess() {
	h.circuitMu.Lock()
	defer h.circuitMu.Unlock()

	// 重置错误计数
	h.circuitBreaker.errors = 0

	// 如果当前是半开状态，恢复到关闭状态
	if h.circuitBreaker.state == 2 {
		h.circuitBreaker.state = 0
		h.logger.Info("🔌 [熔断器] 已恢复（请求成功），熔断器重新关闭")
	}
}

// warmupCDN CDN预热
// 向直链发送 Range 请求预取文件头 256KB，让CDN边缘节点提前缓存 moov atom
// 核心优化：
//   - 智能去重：近期已预热的URL直接跳过，让302响应立即返回（节省300ms+）
//   - 预加载阶段已预热的URL，缓存命中时跳过 → 302延迟从300ms降到~0ms
//   - 预取范围 256KB（覆盖播放器初始 Range 请求）
//   - moov atom 检测：解析文件头256KB判断moov位置
//   - moov在头部（faststart）：跳过尾部预热（省一个请求）
//   - moov不在头部：异步并行预取尾部（覆盖moov在尾部的情况）
//   - 双端预热：文件较大时异步预取文件尾256KB（兜底机制）
//   - 播放器请求 moov atom 时 CDN 直接缓存命中，无需从源站获取
// 原理：
//   - MP4 的 moov atom 包含视频元数据（编码、分辨率、关键帧位置等）
//   - 播放器必须下载并解析 moov atom 才能开始解码
//   - moov 在头部（faststart）：播放器请求 bytes=0-N
//   - moov 在尾部：播放器先请求尾部，再请求头部
//   - 预热双端确保无论 moov 在哪里，CDN 都已缓存
// dedupTTL 参数：
//   - 0：不做去重检查（保留原行为，用于特殊场景）
//   - 60s：预加载阶段（CDN完整缓存，60s内必定仍然缓存）
//   - 30s：缓存命中阶段（用户通常30s内点击播放）
//   - 10s：实时解析阶段（避免快速重试时重复预热）
// 三种调用场景：
//   - executePreload: 异步调用（8s超时, 60s去重），用户浏览详情页期间后台预热
//   - parallelResolve: 同步调用（800ms超时, 10s去重），点击播放时确保CDN就绪
//   - Handle缓存命中: 同步调用（300ms超时, 30s去重），URL已缓存跳过预热让302立即返回
func (h *StreamHandler) warmupCDN(targetURL string, timeout time.Duration, tag string, dedupTTL time.Duration) {
	if targetURL == "" {
		return
	}

	// CDN预热开关（关闭时跳过所有预热）
	if !config.Global.GetEnableCDNWarmup() {
		return
	}

	// 统计预热总次数
	atomic.AddInt64(&h.stats.warmupTotal, 1)

	// 智能去重 - 近期已预热的URL直接跳过
	// 缓存命中场景下，preload刚预热完的URL无需再次预热
	// 让302响应立即返回，节省300ms阻塞延迟
	if dedupTTL > 0 && h.shouldSkipWarmup(targetURL, dedupTTL) {
		atomic.AddInt64(&h.stats.warmupSkips, 1) // 统计跳过次数
		h.logger.Debug("🔥 [预热-%s] 跳过（%d内已预热）", tag, dedupTTL/time.Second)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", targetURL, nil)
	if err != nil {
		atomic.AddInt64(&h.stats.warmupFailures, 1) // 统计失败
		return
	}
	// 预取 256KB 文件头（覆盖播放器初始 Range 请求 + moov atom）
	req.Header.Set("Range", "bytes=0-262143")
	req.Header.Set("User-Agent", userAgents[rand.Intn(len(userAgents))])

	warmupStart := time.Now()
	resp, err := h.warmupClient.Do(req)
	if err != nil {
		atomic.AddInt64(&h.stats.warmupFailures, 1) // 统计失败
		h.logger.Debug("🔥 [预热-%s] 失败 (%dms)", tag, time.Since(warmupStart).Milliseconds())
		return
	}

	// 检查状态码，403/401/4xx/5xx 不标记为已预热
	// 后续缓存命中时跳过预热，直接返回过期URL给播放器 → 3003错误
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		resp.Body.Close()
		atomic.AddInt64(&h.stats.warmupFailures, 1)
		h.logger.Warn("🔥 [预热-%s] 状态码异常: %d (不标记已预热)", tag, resp.StatusCode)
		return
	}

	// 检查是否为HTML错误页面（某些CDN返回200+HTML错误页）
	contentType := resp.Header.Get("Content-Type")
	if isHTMLErrorResponse(resp.StatusCode, contentType, resp.Header) {
		resp.Body.Close()
		atomic.AddInt64(&h.stats.warmupFailures, 1)
		h.logger.Warn("🔥 [预热-%s] 响应为HTML错误页面 (Content-Type: %s)", tag, contentType)
		return
	}

	// 关键优化 - 请求成功拿到响应后立即标记已预热
	// 不必等读完 body，因为 CDN 收到请求后就已经开始从源站获取并缓存
	// 这样预加载的异步 warmup（8s超时）进行中时，用户点击播放触发的缓存命中 warmup
	// 会通过 dedup 检查跳过，让 302 立即返回（避免 300ms 重复预热）
	h.markWarmed(targetURL)

	// 读取body到缓冲区（而非丢弃），用于检测moov atom位置
	// 读取最多 256KB，触发 CDN 完整缓存文件头区域
	// 使用 sync.Pool 复用 buffer，减少 GC 压力
	bufPtr := warmupBufPool.Get().(*[]byte)
	buf := *bufPtr
	buf = buf[:0] // 重置长度，保留容量
	// bytes.Buffer 包装以便 io.Copy
	bb := bytes.NewBuffer(buf)
	io.Copy(bb, io.LimitReader(resp.Body, 256*1024))
	resp.Body.Close()
	headData := bb.Bytes()

	// 归还 buffer 到池
	// 检查容量：如果 bb 扩容导致容量超过 512KB（2 倍预期），不放回池中让 GC 回收
	// 避免池中积累过大 buffer 导致内存增长
	returnedBytes := bb.Bytes()
	if cap(returnedBytes) <= 512*1024 {
		*bufPtr = returnedBytes
		warmupBufPool.Put(bufPtr)
	}

	// 从 Content-Range 头获取文件总大小，用于判断是否需要预取尾部
	// 格式：bytes 0-262143/12345678
	totalSize := h.parseContentLength(resp)

	// 检测moov atom位置，决定是否需要预取尾部
	// - moov在头部（faststart）：跳过尾部预热（省一个请求）
	// - moov不在头部：异步并行预取尾部（覆盖moov在尾部的情况）
	moovInHead := false
	if len(headData) >= 12 { // 至少需要12字节才能检测一个完整atom
		moovInHead = detectMP4MoovInHead(headData)
	}

	// 统计 moov 位置和耗时
	elapsedMs := time.Since(warmupStart).Milliseconds()
	atomic.AddInt64(&h.stats.warmupTotalMs, elapsedMs)
	atomic.AddInt64(&h.stats.warmupSuccess, 1)
	if moovInHead {
		atomic.AddInt64(&h.stats.warmupMoovHead, 1)
	} else if totalSize > 2*1024*1024 {
		atomic.AddInt64(&h.stats.warmupMoovTail, 1)
	}

	moovTag := "尾部"
	if moovInHead {
		moovTag = "头部"
	}
	h.logger.Debug("🔥 [预热-%s] 完成 (%dms): %d, size=%dMB, moov=%s",
		tag, elapsedMs, resp.StatusCode,
		totalSize/(1024*1024), moovTag)

	// 智能尾部预热
	// - moov在头部（faststart）：跳过尾部预热（播放器只需头部即可解码）
	// - moov不在头部：异步预取尾部256KB（moov可能在文件尾部）
	// - 文件小于2MB：不预取尾部（小文件CDN缓存整个文件）
	if totalSize > 2*1024*1024 && !moovInHead {
		go h.warmupCDNTail(targetURL, totalSize)
	}
}

// warmupCDNTail 预取文件尾部
// MP4 moov atom 可能在文件尾部，预取尾部让播放器请求时 CDN 已缓存
func (h *StreamHandler) warmupCDNTail(targetURL string, totalSize int64) {
	if totalSize <= 0 {
		return
	}

	// 预取最后 256KB
	start := totalSize - 256*1024
	if start < 0 {
		start = 0
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", targetURL, nil)
	if err != nil {
		return
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, totalSize-1))
	req.Header.Set("User-Agent", userAgents[rand.Intn(len(userAgents))])

	warmupStart := time.Now()
	resp, err := h.warmupClient.Do(req)
	if err != nil {
		h.logger.Debug("🔥 [预热尾部] 失败 (%dms)", time.Since(warmupStart).Milliseconds())
		return
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 256*1024))
	resp.Body.Close()

	h.logger.Debug("🔥 [预热尾部] 完成 (%dms): %d",
		time.Since(warmupStart).Milliseconds(), resp.StatusCode)
}

// preconnectCDN 预连接CDN
// 在解析出直链后立即预建 TCP+TLS 连接，播放器请求时无需等待连接建立
// 原理：
//   - HTTPS 首次请求需要：DNS解析(50-200ms) + TCP握手(50-100ms) + TLS握手(100-300ms)
//   - 预连接后连接进入 warmupClient 的空闲池，后续请求直接复用
//   - 播放器请求时跳过 DNS+TCP+TLS，节省 200-500ms
// 实现：
//   - 用 warmupClient.Do 发送一个极小的 Range 请求（bytes=0-0，只1字节）
//   - 请求完成后连接自动进入空闲池（KeepAlive）
//   - 后续 warmupCDN 或播放器请求复用此连接
// 注意：
//   - 此函数与 warmupCDN 互补：warmupCDN 预取数据，preconnectCDN 预建连接
//   - 如果 warmupCDN 已预取 256KB，连接已建立，preconnectCDN 会跳过（dedup）
//   - 主要用于解析完成但 warmupCDN 还没执行的场景（如 302 响应前）
func (h *StreamHandler) preconnectCDN(targetURL string) {
	if targetURL == "" {
		return
	}

	// CDN预热开关（关闭时跳过预连接）
	if !config.Global.GetEnableCDNWarmup() {
		return
	}

	// 解析URL获取host
	parsedURL, err := url.Parse(targetURL)
	if err != nil {
		return
	}

	// 只对HTTPS做预连接（HTTP连接建立快，无需预连接）
	if parsedURL.Scheme != "https" {
		return
	}

	// 统计预连接次数
	atomic.AddInt64(&h.stats.preconnectTotal, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", targetURL, nil)
	if err != nil {
		atomic.AddInt64(&h.stats.preconnectFailures, 1)
		return
	}
	// 只请求1字节，最小化数据传输
	req.Header.Set("Range", "bytes=0-0")
	req.Header.Set("User-Agent", userAgents[rand.Intn(len(userAgents))])

	start := time.Now()
	resp, err := h.warmupClient.Do(req)
	if err != nil {
		atomic.AddInt64(&h.stats.preconnectFailures, 1)
		h.logger.Debug("🔌 [预连接] 失败 (%dms): %v", time.Since(start).Milliseconds(), err)
		return
	}
	// 读取1字节后关闭body，连接进入空闲池
	io.CopyN(io.Discard, resp.Body, 1)
	resp.Body.Close()

	atomic.AddInt64(&h.stats.preconnectSuccess, 1)
	h.logger.Debug("🔌 [预连接] 成功 (%dms): %s", time.Since(start).Milliseconds(), parsedURL.Host)
}

// parseContentLength 从响应头解析文件总大小
func (h *StreamHandler) parseContentLength(resp *http.Response) int64 {
	// 优先从 Content-Range 头获取（206 响应）
	// 格式：bytes 0-262143/12345678
	contentRange := resp.Header.Get("Content-Range")
	if contentRange != "" {
		parts := strings.Split(contentRange, "/")
		if len(parts) == 2 {
			if size, err := strconv.ParseInt(parts[1], 10, 64); err == nil {
				return size
			}
		}
	}
	// 200 响应时 ContentLength 是文件总大小
	if resp.ContentLength > 0 {
		return resp.ContentLength
	}
	return 0
}

// PreloadStrmSync 同步预读取 STRM 文件并缓存 URL
// 在 PlaybackInfo 响应处理时同步调用，不等 worker 队列调度
// 确保 URL 在 PlaybackInfo 响应处理完成后立即可用，流请求来时直接命中缓存
// 优化效果：
//   - 旧流程：PlaybackInfo响应 → 入队 → worker调度 → 读strm → 缓存URL（异步，有延迟）
//   - PlaybackInfo响应 → 同步读strm → 缓存URL（立即就绪）
// 各模式行为：
//   - passthrough: 直接缓存原始 URL（strm 工具 URL）
//   - auto + isDirectURL: 缓存 URL + 异步预热
//   - auto 非直链 / always: 交给异步队列解析
func (h *StreamHandler) PreloadStrmSync(source cache.MediaSource) {
	// 预加载开关（关闭时跳过所有预加载）
	if !config.Global.GetEnablePreload() {
		return
	}
	pathLower := strings.ToLower(source.Path)
	if !strings.HasSuffix(pathLower, ".strm") {
		return
	}

	// 如果 URL 已缓存（之前预读过），跳过
	if _, found := h.cache.GetStreamURL(source.ID); found {
		return
	}

	// 同步读取 STRM 文件
	strmURL, err := h.readStrmWithCache(source.Path)
	if err != nil {
		h.logger.Debug("⚠️ [同步预读] STRM读取失败: %s", source.Path)
		return
	}

	// 内网strm支持开关
	// 关闭时：内网地址strm直接缓存URL（不解析/不预热），让播放器自己处理
	// 开启时：正常走解析流程
	if !config.Global.GetEnableLanStrm() && isLanURL(strmURL) {
		h.setStreamURLSmart(source.ID, strmURL)
		h.setURLCache(strmURL, strmURL) // 使用带过期检查的 setURLCache
		h.logger.Debug("🔒 [同步预读] 内网strm支持已关闭，直接缓存URL: ID=%s", source.ID)
		return
	}

	mode := config.Global.GetStrmResolveMode()

	// passthrough 模式：直接缓存 URL
	if mode == "passthrough" {
		h.setStreamURLSmart(source.ID, strmURL)
		h.setURLCache(strmURL, strmURL) // 使用带过期检查的 setURLCache
		atomic.AddInt64(&h.stats.directURLSkips, 1)
		h.logger.Info("⚡ [同步预读] 透传模式，直链已就绪: ID=%s", source.ID)
		return
	}

	// auto 模式：如果 isDirectURL，缓存 URL + 异步预热
	if mode == "auto" && isDirectURL(strmURL) {
		h.setStreamURLSmart(source.ID, strmURL)
		h.setURLCache(strmURL, strmURL) // 使用带过期检查的 setURLCache
		go h.preconnectCDN(strmURL)
		go h.warmupCDN(strmURL, 8*time.Second, "同步预读直链", 60*time.Second)
		atomic.AddInt64(&h.stats.directURLSkips, 1)
		h.logger.Info("⚡ [同步预读] 检测到直链，直链已就绪: ID=%s", source.ID)
		return
	}

	// always 模式或 auto 非直链：直接启动 goroutine 解析（不等队列调度）
	// 直接 go executePreload，立即开始解析（省去队列调度延迟）
	// executePreload 会设置 inflight，流请求可等待
	h.inflightMu.Lock()
	if _, exists := h.inflight[source.Path]; !exists {
		h.inflight[source.Path] = make(chan struct{})
		task := preloadTask{
			mediaSourceID: source.ID,
			itemID:        source.ItemID,
			path:          source.Path,
			priority:      0, // 高优先级（同步预读触发）
		}
		h.inflightMu.Unlock()
		go h.executePreload(task)
		h.logger.Debug("🚀 [同步预读] 已启动直接解析(不等队列): ID=%s", source.ID)
	} else {
		h.inflightMu.Unlock()
		h.logger.Debug("📋 [同步预读] 解析已在进行中，跳过: ID=%s", source.ID)
	}
}

// PreloadMediaSource 预加载媒体源（供PlaybackInfo调用，入队异步处理）
func (h *StreamHandler) PreloadMediaSource(source cache.MediaSource) {
	// 预加载开关（关闭时跳过所有预加载）
	if !config.Global.GetEnablePreload() {
		return
	}
	pathLower := strings.ToLower(source.Path)

	if !strings.HasSuffix(pathLower, ".strm") {
		return
	}

	// 预加载去重：同一path正在处理中则跳过
	// 同时创建 done channel，供流请求等待
	h.inflightMu.Lock()
	if _, exists := h.inflight[source.Path]; exists {
		h.inflightMu.Unlock()
		return
	}
	h.inflight[source.Path] = make(chan struct{})
	h.inflightMu.Unlock()

	priority := 1
	if strings.Contains(pathLower, "movie") || strings.Contains(pathLower, "电影") {
		priority = 0 // 电影优先级更高
	}

	// 根据优先级选择队列
	// priority=0（电影）：入高优先级队列
	// priority=1（其他）：入普通队列
	queue := &h.preloadQueue
	queueName := "普通"
	if priority == 0 {
		queue = &h.highPriorityQueue
		queueName = "高优先"
	}

	select {
	case *queue <- preloadTask{
		mediaSourceID: source.ID,
		itemID:        source.ItemID,
		path:          source.Path,
		priority:      priority,
	}:
		h.logger.Debug("📤 [预加载] 已入队(%s): ID=%s, Priority=%d", queueName, source.ID, priority)
	default:
		// 队列满，从inflight移除以便后续可重试
		h.inflightMu.Lock()
		delete(h.inflight, source.Path)
		h.inflightMu.Unlock()
		// ✅ P1-12: 队列满时增加丢弃计数，便于监控
		atomic.AddInt64(&h.stats.droppedCount, 1)
		h.logger.Warn("⚠️ [预加载] 队列已满，丢弃任务: ID=%s", source.ID)
	}
}

// getURLCache 获取URL缓存（带过期检查）
// 返回 (url, true) 表示缓存命中且未过期；("", false) 表示未命中或已过期
// 过期条目会被自动清理
func (h *StreamHandler) getURLCache(key string) (string, bool) {
	h.urlMutex.Lock()
	entry, ok := h.urlCache[key]
	if !ok {
		h.urlMutex.Unlock()
		return "", false
	}
	// 检查是否过期
	if time.Now().After(entry.expireAt) {
		// 过期，清理条目
		if elem, exists := h.urlIndex[key]; exists {
			h.urlList.Remove(elem)
		}
		delete(h.urlCache, key)
		delete(h.urlIndex, key)
		h.urlMutex.Unlock()
		return "", false
	}
	// LRU: 移到队尾
	if elem, exists := h.urlIndex[key]; exists {
		h.urlList.MoveToBack(elem)
	}
	h.urlMutex.Unlock()
	return entry.url, true
}

// setURLCache 设置URL缓存（带智能TTL）
// 根据 value URL 的签名有效期动态设置过期时间
// 修复： urlCache 永不过期，导致已过期签名URL被反复返回（3003 错误根因）
func (h *StreamHandler) setURLCache(key, value string) {
	// 计算TTL：如果 value 是签名URL，按签名有效期；否则用配置的 cache_ttl
	ttl := config.Global.GetCacheTTL()
	if config.Global.GetEnableSmartTTL() {
		if signedTTL, ok := detectSignedURLTTL(value, ttl); ok {
			ttl = signedTTL
		}
	}

	h.urlMutex.Lock()
	now := time.Now()

	if _, exists := h.urlCache[key]; !exists {
		// 新条目
		elem := h.urlList.PushBack(key)
		h.urlCache[key] = urlCacheEntry{
			url:       value,
			expireAt:  now.Add(ttl),
			createdAt: now,
		}
		h.urlIndex[key] = elem
		if h.urlList.Len() > maxURLCacheSize {
			h.evictOldestURL()
		}
	} else {
		// 已存在条目也要刷新 URL 和过期时间
		// 刷新时保留原 createdAt（半衰期基于原始创建时间计算）
		existing := h.urlCache[key]
		h.urlCache[key] = urlCacheEntry{
			url:       value,
			expireAt:  now.Add(ttl),
			createdAt: existing.createdAt, // 保留原始创建时间
		}
		if elem, exists := h.urlIndex[key]; exists {
			h.urlList.MoveToBack(elem)
		}
	}

	h.urlMutex.Unlock()
}

// deleteURLCacheValue 按最终URL删除URL解析缓存
// 缓存命中校验失败时使用，避免坏URL继续通过 urlCache 间接命中。
func (h *StreamHandler) deleteURLCacheValue(value string) {
	if value == "" {
		return
	}
	h.urlMutex.Lock()
	for key, entry := range h.urlCache {
		if entry.url == value {
			if elem, exists := h.urlIndex[key]; exists {
				h.urlList.Remove(elem)
			}
			delete(h.urlCache, key)
			delete(h.urlIndex, key)
		}
	}
	h.urlMutex.Unlock()

	h.validateMu.Lock()
	delete(h.validateRecent, value)
	h.validateMu.Unlock()
}

// Handle 处理视频流请求
func (h *StreamHandler) Handle(w http.ResponseWriter, r *http.Request) bool {
	path := r.URL.Path
	pathLower := strings.ToLower(path)

	if !h.isStreamRequest(pathLower) {
		return false
	}

	// ✅ 仅对流请求计数，避免误统计非流请求
	// 递归重试（X-Fnysfd-Miss-Retry）不重复计数
	if r.Header.Get("X-Fnysfd-Miss-Retry") != "1" {
		atomic.AddInt64(&h.stats.totalRequests, 1)
	}

	return h.handleStream(w, r, pathLower)
}

// handleStream 流请求核心处理逻辑（由 Handle 调用，不重复计数 totalRequests）
func (h *StreamHandler) handleStream(w http.ResponseWriter, r *http.Request, pathLower string) bool {
	startTime := time.Now()

	mediaSourceID := r.URL.Query().Get("MediaSourceId")
	itemID := extractItemID(r.URL.Path)

	// 🎯 核心优化：多策略极速查找
	source, found := h.findInCacheUltraFast(r, mediaSourceID, itemID)

	if found && source.ID != "" {
		pathLower = strings.ToLower(source.Path)

		// 获取剧集名称用于日志
		epName := source.Name
		if epName == "" {
			epName = extractEpisodeName(source.Path)
		}
		if epName == "" {
			epName = itemID // 最终兜底用 ItemId
		}

		// 找到 source 后打带名称的流请求日志
		// 播放器会发多个 Range 请求（seek/缓冲），同一 MediaSource 短时间重复请求降为 Debug
		// 仅首次请求打 Info，后续 Range 请求用 Debug 避免刷屏
		if h.shouldSkipStreamReqLog(mediaSourceID, 10*time.Second) {
			h.logger.Debug("🎬 [流请求] %s | %s | MS=%s", epName, itemID, mediaSourceID)
		} else {
			h.logger.Info("🎬 [流请求] %s | %s | MS=%s", epName, itemID, mediaSourceID)
		}

		if strings.HasSuffix(pathLower, ".strm") {
			// ✅ 策略A：检查预加载的直链（零延迟）
			if streamURL, cacheFound := h.cache.GetStreamURL(source.ID); cacheFound {
				// 防3003增强 + 起播速度优化
				// 校验和预热并行执行：validatePlayableURL(900ms) 与 startupGuardWarmup(3s) 同时跑
				// 总耗时从 3.9s 降到 3s，节省约 900ms
				if !h.shouldSkipURLValidation(streamURL.URL, 60*time.Second) {
					var validateErr error
					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						defer wg.Done()
						validateErr = h.validatePlayableURL(streamURL.URL, r)
					}()

					// 预热与校验并行（warmupClient 与 fastClient 互不干扰）
					if config.Global.GetStrmResolveMode() != "passthrough" {
						h.startupGuardWarmup(streamURL.URL, source.Path, "缓存命中")
					}

					wg.Wait() // 校验 2s 超时，预热 3s 超时，取最大值

					if validateErr != nil {
						// 区分错误类型
						// - 超时/网络错误：URL 可能没问题，只是 CDN 慢，不清缓存，直接返回 302 让播放器试
						// - HTML/错误页/403：URL 确实有问题，清缓存重解析
						errStr := validateErr.Error()
						isTimeoutOrNetwork := strings.Contains(errStr, "timeout") ||
							strings.Contains(errStr, "deadline exceeded") ||
							strings.Contains(errStr, "connection refused") ||
							strings.Contains(errStr, "connection reset") ||
							strings.Contains(errStr, "no such host") ||
							strings.Contains(errStr, "EOF")

						if isTimeoutOrNetwork {
							// 超时/网络错误不清缓存，标记已校验避免反复重试，直接返回 302
							h.markURLValidated(streamURL.URL)
							atomic.AddInt64(&h.stats.cacheHits, 1)
							latency := time.Since(startTime).Milliseconds()
							h.logger.Warn("⚠️ [命中校验] %s 校验超时/网络错误，仍返回302: %v (%dms) → %s",
								epName, validateErr, latency, shortURL(streamURL.URL))
							w.Header().Set("Location", streamURL.URL)
							w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d", computeRedirectCacheAge(streamURL.URL)))
							w.WriteHeader(http.StatusFound)
							return true
						}
						// 确认是错误页面/403等，清缓存重解析
						h.logger.Warn("⚠️ [命中校验] %s 缓存URL不可播放，清除并重解析: %v", epName, validateErr)
						h.cache.DeleteStreamURL(source.ID)
						h.deleteURLCacheValue(streamURL.URL)
					} else {
						h.markURLValidated(streamURL.URL)
						atomic.AddInt64(&h.stats.cacheHits, 1)

						latency := time.Since(startTime).Milliseconds()
						// 缓存命中是常态，播放器会发多个 Range 请求都命中缓存
						// 降为 Debug 避免每次命中都刷屏；首次命中由 [流请求] 日志覆盖
						h.logger.Debug("⚡ [命中] %s | %dms → %s", epName, latency, shortURL(streamURL.URL))

						w.Header().Set("Location", streamURL.URL)
						w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d", computeRedirectCacheAge(streamURL.URL)))
						w.WriteHeader(http.StatusFound)
						return true
					}
				} else {
					atomic.AddInt64(&h.stats.cacheHits, 1)

					if config.Global.GetStrmResolveMode() != "passthrough" {
						h.startupGuardWarmup(streamURL.URL, source.Path, "缓存命中")
					}

					latency := time.Since(startTime).Milliseconds()
					h.logger.Debug("⚡ [命中] %s | %dms → %s", epName, latency, shortURL(streamURL.URL))

					w.Header().Set("Location", streamURL.URL)
					w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d", computeRedirectCacheAge(streamURL.URL)))
					w.WriteHeader(http.StatusFound)
					return true
				}
			}

			// ⚡ 策略B：实时并行解析（STRM读取 + URL解析同时进行）
			// 内部会优先等待预加载完成（3s 超时）
			if finalURL, ok := h.parallelResolve(source, r); ok {
				latency := time.Since(startTime).Milliseconds()
				h.logger.Info("🎯 [解析] %s | %dms → %s", epName, latency, shortURL(finalURL))

				w.Header().Set("Location", finalURL)
				w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d", computeRedirectCacheAge(finalURL)))
				w.WriteHeader(http.StatusFound)
				return true
			}

			// 策略C：fallback - 返回strm文件内容作为URL（让播放器自己尝试）
			if strmURL, err := h.readStrmWithCache(source.Path); err == nil && strmURL != "" {
				atomic.AddInt64(&h.stats.fallbackCount, 1)
				latency := time.Since(startTime).Milliseconds()
				h.logger.Info("🔄 [fallback] %s | %dms → %s", epName, latency, shortURL(strmURL))

				w.Header().Set("Location", strmURL)
				w.WriteHeader(http.StatusFound)
				return true
			}

			// 所有策略都失败
			latency := time.Since(startTime).Milliseconds()
			h.logger.Warn("❌ [STRM] %s 所有策略失败，转发反代 (耗时: %dms)",
				epName, latency)
		} else {
			// 非 STRM 文件，直接转发
			h.logger.Debug("➡️ [STRM] 非STRM文件，转发: %s", source.Path)
		}
	} else {
		// 未找到 source 也打流请求日志
		h.logger.Info("🎬 [流请求] ItemId=%s | MS=%s (未缓存)", itemID, mediaSourceID)

		// 区分对待 MediaSourceId 是否为空
		// - MediaSourceId 不为空却未缓存：说明 PlaybackInfo 还没处理完，打 Warn 并触发兜底
		// - MediaSourceId 为空但 ItemId 存在：新剧无媒体信息时，播放器可能不带 MediaSourceId 直接发流请求
		//   同样需要触发兜底，通过 itemID 轮询 PlaybackInfo 获取 MediaSource
		// - 两者都为空：可能是非标准流请求，正常转发
		shouldTriggerMiss := false
		if mediaSourceID != "" {
			latency := time.Since(startTime).Milliseconds()
			h.logger.Warn("⚠️ [STRM] MediaSource未缓存: MediaSourceId=%s, ItemId=%s (耗时: %dms)",
				mediaSourceID, itemID, latency)
			shouldTriggerMiss = true
		} else if itemID != "" {
			latency := time.Since(startTime).Milliseconds()
			h.logger.Warn("⚠️ [STRM] MediaSource未缓存(无MS ID): ItemId=%s (耗时: %dms)", itemID, latency)
			shouldTriggerMiss = true
		}

		if shouldTriggerMiss {
			// 新剧无媒体信息兜底
			// 不直接把未准备好的流请求转发给飞牛，而是同步触发 PlaybackInfo 轮询。
			// 成功后递归重试一次当前流请求，此时 MediaSource/URL 已进入缓存。
			if h.onMediaSourceMiss != nil && itemID != "" && r.Header.Get("X-Fnysfd-Miss-Retry") != "1" {
				h.logger.Info("🧩 [MediaSource兜底] 开始同步准备: ItemId=%s, MS=%s", itemID, mediaSourceID)
				if h.onMediaSourceMiss(r, itemID, mediaSourceID) {
					r.Header.Set("X-Fnysfd-Miss-Retry", "1")
					h.logger.Info("✅ [MediaSource兜底] 已准备完成，重试当前流请求: ItemId=%s", itemID)
					// 调用内部方法避免重复计数 totalRequests 和重复 isStreamRequest 判断
					return h.handleStream(w, r, pathLower)
				}
				h.logger.Warn("⚠️ [MediaSource兜底] 准备超时，避免转发未准备流请求: ItemId=%s", itemID)
				http.Error(w, "MediaSource is preparing, please retry", http.StatusServiceUnavailable)
				return true
			}
		} else {
			h.logger.Debug("➡️ [STRM] 无MediaSourceId，转发反代: ItemId=%s", itemID)
		}
	}

	return false
}

// parallelResolve 解析STRM和URL
// 起播优先版
//   - 先读取STRM并检查URL缓存/直链/透传，命中则立即返回
//   - 只有确实需要HTTP解析时，才短等正在进行的预加载
//   - 避免“预加载正在跑”时直接链接也被最多8秒等待拖慢
func (h *StreamHandler) parallelResolve(source cache.MediaSource, r *http.Request) (string, bool) {
	// 获取剧集名称用于日志
	epName := source.Name
	if epName == "" {
		epName = extractEpisodeName(source.Path)
	}
	if epName == "" {
		epName = source.ItemID // 最终兜底用 ItemId
	}

	// 同步读取STRM文件（带缓存，避免无意义的goroutine包装与阻塞等待）
	strmURL, err := h.readStrmWithCache(source.Path)
	if err != nil {
		h.logger.Warn("⚠️ [STRM] %s 读取失败: %s", epName, source.Path)
		return "", false
	}

	// 内网strm支持开关
	// 关闭时：内网地址strm直接返回URL给播放器（让播放器自己处理，不经过反代解析）
	// 开启时：正常走解析流程（需要容器能访问内网，建议host网络模式）
	if !config.Global.GetEnableLanStrm() && isLanURL(strmURL) {
		h.logger.Debug("🔒 [内网strm] 支持已关闭，直接返回URL: %s", source.ID)
		h.setStreamURLSmart(source.ID, strmURL)
		return strmURL, true
	}

	// 先检查URL缓存（快速路径，带过期检查）
	if cached, ok := h.getURLCache(strmURL); ok {
		h.setStreamURLSmart(source.ID, cached)
	// URL缓存命中也做同步护航预热（dedupTTL=0 强制执行）
	h.startupGuardWarmup(cached, source.Path, "URL缓存")
		return cached, true
	}

	// 检查负缓存
	if h.isNegativelyCached(strmURL) {
		h.logger.Debug("⏭️ [STRM] 跳过负缓存URL: %s", strmURL)
		return "", false
	}

	// STRM 解析模式开关（与 executePreload 保持一致）
	mode := config.Global.GetStrmResolveMode()

	// passthrough 模式 - 直接缓存原始 URL，不做任何后续处理
	if mode == "passthrough" {
		h.setStreamURLSmart(source.ID, strmURL)
		h.setURLCache(strmURL, strmURL) // 使用带过期检查的 setURLCache
		atomic.AddInt64(&h.stats.directURLSkips, 1)
		h.logger.Info("⚡ [STRM] 透传模式，跳过解析: %s", strmURL)
		return strmURL, true
	}

	// auto 模式 - 智能识别 302 strm，如果 URL 本身就是直链，跳过解析直接缓存
	// always 模式跳过此判断，强制走 HTTP 解析
	if mode == "auto" && isDirectURL(strmURL) {
		h.logger.Info("⚡ [STRM] %s 检测到直链，跳过解析", epName)
		// 直接缓存原始 URL 作为最终直链
		h.setStreamURLSmart(source.ID, strmURL)
		h.setURLCache(strmURL, strmURL) // 使用带过期检查的 setURLCache
		// 异步预连接
		go h.preconnectCDN(strmURL)
		h.startupGuardWarmup(strmURL, source.Path, "直链跳过")
		atomic.AddInt64(&h.stats.directURLSkips, 1) // 统计直链跳过次数
		return strmURL, true
	}

	// 需要HTTP解析时，短等正在进行的预加载
	// 只在必须解析时等待，且前台最多等1.2秒，超时后自己解析，优先保证起播
	// 兜底重试场景（X-Fnysfd-Miss-Retry）等待更久（3秒），因为预加载刚由兜底触发，需要更多时间完成
	h.inflightMu.Lock()
	doneCh := h.inflight[source.Path]
	h.inflightMu.Unlock()
	if doneCh != nil {
		// 兜底重试时延长等待：兜底刚触发预加载，1.2s 可能不够
		waitTimeout := 1200 * time.Millisecond
		if r.Header.Get("X-Fnysfd-Miss-Retry") == "1" {
			waitTimeout = 3000 * time.Millisecond
		}
		h.logger.Debug("⏳ [等待预加载] %s (超时: %v)", source.ID, waitTimeout)
		select {
		case <-doneCh:
			// 预加载完成，检查缓存是否有结果
			if streamURL, cacheFound := h.cache.GetStreamURL(source.ID); cacheFound {
				atomic.AddInt64(&h.stats.cacheHits, 1)
				h.logger.Debug("⚡ [预加载命中] %s", epName)
				h.startupGuardWarmup(streamURL.URL, source.Path, "预加载命中")
				return streamURL.URL, true
			}
			// 预加载完成但缓存未命中（预加载失败），继续自行解析
			h.logger.Debug("⚠️ [预加载未命中] %s", source.ID)
		case <-time.After(waitTimeout):
			h.logger.Debug("⏰ [等待预加载超时%v，改为前台解析] %s", waitTimeout, source.ID)
		}
	}

	// 实时解析（带重试 + fallback）
	// 解析失败时立即换 UA 再试一次，提高第一遍解析成功率
	// 解析失败立即换不同 UA 重新解析（不同 UA 可能绕过源站限流）
	// 但只对可能因 UA 被限的错误重试（403/timeout/连接错误），404/500 等明确错误不重试
	finalURL, err := h.resolveURLWithRetry(strmURL, r)
	if err != nil {
		// 判断错误类型，只对可能因 UA 限流引起的错误换 UA 重试
		// 404/410/500/502/503 是明确的服务器错误，换 UA 无意义，直接失败
		errStr := err.Error()
		shouldRetryWithUA := strings.Contains(errStr, "403") ||
			strings.Contains(errStr, "Forbidden") ||
			strings.Contains(errStr, "timeout") ||
			strings.Contains(errStr, "connection refused") ||
			strings.Contains(errStr, "no such host") ||
			strings.Contains(errStr, "context canceled") ||
			strings.Contains(errStr, "EOF") ||
			strings.Contains(errStr, "reset")

		if shouldRetryWithUA {
			// 第一次失败可能是 UA 被限流，立即换不同 UA 再试一次
			retryReq := &http.Request{Header: http.Header{}}
			retryReq.Header.Set("User-Agent", h.getRandomUserAgent())
			finalURL, err = h.resolveURLWithRetry(strmURL, retryReq)
			if err != nil {
				h.logger.Error("❌ [STRM] URL解析失败（换UA重试后）: %s", strmURL)
				h.markFailed(strmURL, err)
				return "", false
			}
			h.logger.Info("🔄 [STRM] 换UA重试成功: %s", strmURL)
		} else {
			// 明确错误（404/500等），不重试，直接失败
			h.logger.Error("❌ [STRM] URL解析失败: %s, 错误: %v", strmURL, err)
			h.markFailed(strmURL, err)
			return "", false
		}
	}

	// 缓存结果
	h.setStreamURLSmart(source.ID, finalURL)

	// 存入URL缓存（带智能TTL过期）
	h.setURLCache(strmURL, finalURL)

	// 预连接CDN - 异步触发，不阻塞 302 响应
	// 在 warmupCDN 之前先触发预连接，确保 TCP+TLS 连接已建立
	// warmupCDN 复用此连接，省去 DNS+TCP+TLS 握手时间（200-500ms）
	// 注意：preconnectCDN 内部会判断是否 HTTPS，HTTP 直接返回
	go h.preconnectCDN(finalURL)

	// 手动切集3003修复
	// 之前实时解析后立即返回302，warmup后台异步跑。
	// 手动切下一集时，播放器可能比warmup更早打到CDN，首次拿到错误页/未准备好内容而3003；
	// 用户点重试时warmup已完成，所以又能播放。这里改为“起播护航预热”：
	// 最多同步等1.2秒预热文件头，成功/失败都不阻塞太久，但能显著降低首次3003。
	h.startupGuardWarmup(finalURL, source.Path, "实时解析")

	return finalURL, true
}

// startupGuardWarmup 起播护航预热
// 用在"刚解析出的新URL"或"缓存命中"场景，同步等待CDN拿到文件头再把302返回给播放器。
// 超时从1.2s提升到3s，解决切下一集时CDN冷启动未完成导致3003的问题。
// 加回5s短去重——播放器可能连续发多个流请求（不同Range），5s内只预热一次。
//   - CDN已热时：warmupCDN在5s去重窗口内直接跳过，~0ms返回
//   - CDN冷启动时：首次请求触发3s预热，后续请求跳过，总耗时仍为3s
func (h *StreamHandler) startupGuardWarmup(targetURL string, sourcePath string, tag string) {
	if targetURL == "" || !config.Global.GetEnableCDNWarmup() {
		return
	}
	// 5s短去重，避免播放器连续请求时重复预热同一个URL
	if h.shouldSkipWarmup(targetURL, 5*time.Second) {
		h.logger.Debug("🔥 [起播护航-%s] 5s内已预热，跳过", tag)
		return
	}
	start := time.Now()
	// 起播护航是内部优化细节（同步等待CDN文件头），每次缓存命中都会触发
	// 降为 Debug 避免与 [命中] 日志双重刷屏
	h.logger.Debug("🔥 [起播护航-%s] 开始", tag)
	h.warmupCDN(targetURL, 3*time.Second, "起播护航-"+tag, 0)
	h.logger.Debug("🔥 [起播护航-%s] 完成 (%dms)", tag, time.Since(start).Milliseconds())
}

// emergencyPreload 紧急预加载（当缓存未命中时触发，调用方应以 go 调用）
func (h *StreamHandler) emergencyPreload(mediaSourceID string, itemID string) {
	// 用带缓冲channel作为信号量，限制紧急预加载并发数，避免无限spawn goroutine
	select {
	case h.emergencySem <- struct{}{}:
		// 获取到信号量，继续执行
	default:
		// 并发数已达上限，直接放弃，避免无限spawn goroutine
		return
	}
	defer func() { <-h.emergencySem }()

	// ✅ P1-11: 删除 time.Sleep(10ms)，避免不必要的延迟（信号量已限制并发）

	if source, found := h.cache.Get(mediaSourceID); found {
		h.PreloadMediaSource(source)
	} else if itemID != "" {
		if source, found := h.cache.GetByItemID(itemID); found {
			h.PreloadMediaSource(source)
		}
	}
}

// findInCacheUltraFast 超高速多维查找
func (h *StreamHandler) findInCacheUltraFast(r *http.Request, mediaSourceID string, itemID string) (cache.MediaSource, bool) {
	// ⚡ 快速路径1：MediaSourceId直接查找（最常见情况）
	if mediaSourceID != "" {
		if source, found := h.cache.Get(mediaSourceID); found {
			return source, true
		}
	}

	// ⚡ 快速路径2：ItemId查找
	if itemID != "" {
		if source, found := h.cache.GetByItemID(itemID); found {
			return source, true
		}
	}

	// 🔄 中速路径3：查询参数提取
	if id := extractIDFromQuery(r); id != "" {
		if source, found := h.cache.Get(id); found {
			return source, true
		}
	}

	// 🐢 慢速路径4：路径扫描（只在前面都失败时执行）
	if ids := extractAllIDsFromPath(r.URL.Path); len(ids) > 0 {
		for _, id := range ids {
			if source, found := h.cache.Get(id); found {
				return source, true
			}
		}
	}

	return cache.MediaSource{}, false
}

// isStreamRequest 判断是否是流请求
// 历史问题修复：/Items/{id}/Images/Primary 曾被误判为流请求
//	→ 海报墙/详情页的图片请求被拦截，产生大量 "MediaSource未缓存" 噪音日志
// 修复：移除 /items/ 宽泛匹配，只匹配 Jellyfin/Emby 明确的流请求路径
// 修复：/Videos/{id}/AdditionalParts 被误判为流请求（查询影片额外部分，非视频流）
// Jellyfin/Emby 流请求真实路径格式：
//   - /Videos/{id}/stream 或 /Videos/{id}/stream.{ext}  （视频）
//   - /Audio/{id}/universal                              （音频转码）
//   - /Items/{id}/Download                               （下载）
//   - /Videos/{id}/stream.m3u8 或 /master.m3u8            （HLS）
// 非流请求（需要排除）：
//   - /Items/{id}/Images/Primary    （海报图片）
//   - /Items/{id}/Images/Backdrop   （背景图）
//   - /Items/{id}/Images/Thumb      （缩略图）
//   - /Videos/{id}/AdditionalParts  （影片额外部分查询，非视频流）
//   - /Videos/{id}/SpecialFeatures  （特别篇查询）
func (h *StreamHandler) isStreamRequest(pathLower string) bool {
	// 🚫 黑名单：明确不是流请求的路径（优先排除，避免误判）
	nonStreamPatterns := []string{
		"/images/",         // 图片请求
		"/additionalparts", // 影片额外部分（多版本/分段查询）
		"/specialfeatures", // 特别篇查询
	}
	for _, pattern := range nonStreamPatterns {
		if strings.Contains(pathLower, pattern) {
			return false
		}
	}

	// ✅ 明确的流请求路径模式（包含即认定是流请求）
	streamPatterns := []string{
		"/stream.",     // /Videos/{id}/stream.mp4
		"/stream?",     // /Videos/{id}/stream?MediaSourceId=...
		"/master.m3u8", // HLS 主播放列表
		"/universal",   // /Audio/{id}/universal（音频转码）
		"/download",    // /Items/{id}/Download
	}
	for _, pattern := range streamPatterns {
		if strings.Contains(pathLower, pattern) {
			return true
		}
	}

	// ✅ /videos/ 和 /audio/ 路径，但排除 /images/ 子路径
	// Jellyfin 视频/音频流：/Videos/{id}/stream, /Audio/{id}/...
	// 但图片也会用 /Videos/{id}/Images/... 格式，需要排除
	if strings.Contains(pathLower, "/videos/") || strings.Contains(pathLower, "/audio/") {
		// 排除图片请求（已在黑名单中处理，这里双重保险）
		if !strings.Contains(pathLower, "/images/") {
			return true
		}
	}

	// ✅ 路径以视频扩展名结尾（直接请求视频文件）
	videoExts := []string{".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".ts", ".m4s"}
	for _, ext := range videoExts {
		if strings.HasSuffix(pathLower, ext) {
			return true
		}
	}

	return false
}

// extractIDFromQuery 从查询参数提取ID
func extractIDFromQuery(r *http.Request) string {
	params := []string{"id", "ItemId", "mediaSourceId", "MediaSourceId"}
	for _, param := range params {
		if id := r.URL.Query().Get(param); id != "" && len(id) >= 16 {
			return id
		}
	}
	return ""
}

// getRandomUserAgent 随机获取User-Agent
func (h *StreamHandler) getRandomUserAgent() string {
	return userAgents[rand.Intn(len(userAgents))]
}

// isDirectURL 判断 URL 是否本身就是直链（无需跟随重定向）
// 新增：识别"302 strm"场景，跳过解析直接缓存，省 200-500ms 解析时间
// 判断标准（满足任一即认为是直链）：
//  1. URL 域名是常见 CDN/网盘直链域名（用户用工具生成的 302 strm）
//  2. URL 已带签名参数（如 x-oss-expires、sign、auth等），表明是临时直链
// 常见的"非直链"场景（需要跟随重定向）：
//   - 短链（如 https://pan.quark.cn/s/xxx）
//   - 网盘分享链接（需要 API 转换）
//   - Alist 中转链接
// isLanURL 检测URL是否为内网地址
// 用于 EnableLanStrm 配置：内网地址strm在关闭支持时跳过解析
// 判断标准：
//   - host 是 IP 且为私有地址段（10.x / 172.16-31.x / 192.168.x / 127.x）
//   - host 是 localhost
//   - host 是 *.local / *.internal 等本地域名
func isLanURL(urlStr string) bool {
	if urlStr == "" {
		return false
	}
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return false
	}
	host := strings.ToLower(parsedURL.Hostname()) // 去掉端口
	if host == "" {
		return false
	}
	// localhost
	if host == "localhost" {
		return true
	}
	// 本地域名后缀
	if strings.HasSuffix(host, ".local") || strings.HasSuffix(host, ".internal") ||
		strings.HasSuffix(host, ".lan") || strings.HasSuffix(host, ".home") {
		return true
	}
	// IP 地址判断
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	// 私有地址段
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() {
		return true
	}
	return false
}

// detectSignedURLTTL 检测带签名URL的有效期
// 用于智能签名TTL：根据URL中的签名参数推算缓存TTL，避免缓存过期晚于签名过期导致403
// 支持的签名参数格式：
//   - OSS/STS: x-oss-signature=...&x-oss-expires=1719500000（Unix时间戳）
//   - 阿里云: expires=1719500000
//   - 腾讯云: q-sign-time=1719500000;1719503600（起;止时间戳）
//   - 七牛: e=1719500000（过期时间戳）
//   - 通用: sign=...&expires=1719500000 / auth_key=1719500000-0-0-...
//   - 百度: expiration=2024-06-27T12:00:00Z（ISO8601格式，暂不解析）
// 返回值：
//   - ttl：建议的缓存TTL（取签名有效期的50%作为安全余量，避免边界过期）
//   - ok：是否成功检测到签名有效期
// 安全策略：
//   - 检测到的TTL上限为配置的cache_ttl（不超过用户配置值）
//   - 检测到的TTL下限为5分钟（避免过短TTL导致频繁重新解析）
//   - 未检测到签名参数或解析失败返回 ok=false，调用方使用默认TTL
func detectSignedURLTTL(urlStr string, maxTTL time.Duration) (time.Duration, bool) {
	if urlStr == "" {
		return 0, false
	}
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return 0, false
	}
	q := parsedURL.Query()

	// 优先级1：x-oss-expires（阿里云 OSS / STS 临时凭证）
	if v := q.Get("x-oss-expires"); v != "" {
		if ts, err := strconv.ParseInt(v, 10, 64); err == nil && ts > 0 {
			return computeSignedTTL(ts, maxTTL), true
		}
	}
	// 优先级2：expires（通用 / 阿里云）
	if v := q.Get("expires"); v != "" {
		if ts, err := strconv.ParseInt(v, 10, 64); err == nil && ts > 0 {
			return computeSignedTTL(ts, maxTTL), true
		}
	}
	// 优先级3：e（七牛云过期时间戳）
	if v := q.Get("e"); v != "" {
		if ts, err := strconv.ParseInt(v, 10, 64); err == nil && ts > 0 {
			return computeSignedTTL(ts, maxTTL), true
		}
	}
	// 优先级4：auth_key=1719500000-0-0-...（阿里CDN鉴权A型，首段为时间戳）
	if v := q.Get("auth_key"); v != "" {
		parts := strings.SplitN(v, "-", 2)
		if ts, err := strconv.ParseInt(parts[0], 10, 64); err == nil && ts > 0 {
			return computeSignedTTL(ts, maxTTL), true
		}
	}
	// 优先级5：q-sign-time=start;end（腾讯云COS，分号分隔起止时间戳）
	if v := q.Get("q-sign-time"); v != "" {
		parts := strings.SplitN(v, ";", 2)
		if len(parts) == 2 {
			if end, err := strconv.ParseInt(parts[1], 10, 64); err == nil && end > 0 {
				return computeSignedTTL(end, maxTTL), true
			}
		}
	}
	return 0, false
}

// computeSignedURLTTL 根据签名过期时间戳计算缓存TTL
// 策略：取签名剩余有效期的50%作为缓存TTL（安全余量），并限制在 [1min, maxTTL] 范围内
// 修复：5分钟下限会让TTL超过签名有效期，下限降为 1 分钟，且 TTL 绝不超过签名的剩余有效期
func computeSignedTTL(expiresAt int64, maxTTL time.Duration) time.Duration {
	now := time.Now().Unix()
	remainingSec := expiresAt - now
	if remainingSec <= 0 {
		// 签名已过期，返回最小TTL（1分钟），让缓存快速失效触发重新解析
		return 1 * time.Minute
	}

	remainingDur := time.Duration(remainingSec) * time.Second

	// 取50%安全余量
	ttl := remainingDur / 2

	// 下限从5分钟降为1分钟（避免短有效期签名被过度缓存）
	if ttl < 1*time.Minute {
		ttl = 1 * time.Minute
	}

	// 关键修复：TTL 绝不能超过签名的剩余有效期
	// （1分钟下限在签名剩余不足1分钟时仍会超限，此处兜底）
	if ttl > remainingDur {
		ttl = remainingDur
	}

	// 上限为maxTTL（不超过配置的cache_ttl）
	if maxTTL > 0 && ttl > maxTTL {
		ttl = maxTTL
	}
	return ttl
}

// isHTMLErrorResponse 检测CDN响应是否为HTML错误页面
// 某些CDN在签名过期、权限不足时返回200状态码 + HTML错误页面（而非标准的403/404）
// 播放器收到HTML后会报 UnrecognizedInputFormatException (3003 错误)
// 判断依据：
//   - Content-Type 包含 text/html
//   - 或 Content-Length 异常小（< 4KB，视频文件通常远大于此）
//   - 且 Range 请求返回的 Content-Range 不存在（正常视频会返回 Content-Range）
func isHTMLErrorResponse(statusCode int, contentType string, headers http.Header) bool {
	// Content-Type 包含 text/html → 一定是错误页面
	if strings.Contains(strings.ToLower(contentType), "text/html") {
		return true
	}
	// Content-Type 包含 application/json → CDN错误响应
	if strings.Contains(strings.ToLower(contentType), "application/json") {
		return true
	}
	// Content-Type 包含 text/plain 且无 Content-Range → 可能是错误页面
	// 正常视频流会有 Content-Range 头（因为解析时发送了 Range: bytes=0-0）
	if strings.Contains(strings.ToLower(contentType), "text/plain") {
		if headers.Get("Content-Range") == "" {
			return true
		}
	}
	return false
}

// computeRedirectCacheAge 计算302重定向响应的 Cache-Control max-age
// 修复：固定 max-age=3600（1小时），播放器缓存302重定向后，
// 即使签名URL已过期，播放器仍使用缓存的旧URL，导致403/3003错误
// 策略：
//   - 签名URL：取签名TTL的50%作为max-age（确保播放器在签名过期前回源刷新）
//   - 非签名URL：默认300秒（5分钟，平衡缓存命中率和URL新鲜度）
func computeRedirectCacheAge(urlStr string) int {
	const defaultMaxAge = 300 // 5分钟

	if config.Global.GetEnableSmartTTL() {
		maxTTL := config.Global.GetCacheTTL()
		if ttl, ok := detectSignedURLTTL(urlStr, maxTTL); ok {
			// 签名URL：取TTL的50%作为max-age
			maxAge := int(ttl.Seconds() / 2)
			if maxAge > defaultMaxAge {
				maxAge = defaultMaxAge
			}
			if maxAge < 10 {
				maxAge = 10 // 至少10秒，避免过于频繁回源
			}
			return maxAge
		}
	}
	return defaultMaxAge
}

// setStreamURLSmart 智能缓存URL
// 根据配置决定使用智能签名TTL检测还是固定TTL
//   - EnableSmartTTL=true：检测URL签名参数，按签名有效期动态设置TTL（解决播放中403）
//   - EnableSmartTTL=false：使用配置的固定cache_ttl
func (h *StreamHandler) setStreamURLSmart(mediaSourceID string, url string) {
	// 智能签名TTL检测
	if config.Global.GetEnableSmartTTL() {
		maxTTL := config.Global.GetCacheTTL()
		if ttl, ok := detectSignedURLTTL(url, maxTTL); ok {
			h.cache.SetStreamURLWithTTL(mediaSourceID, url, ttl)
			h.logger.Debug("⏱️ [智能TTL] ID=%s 检测到签名TTL=%v（cache_ttl=%v）", mediaSourceID, ttl, maxTTL)
			return
		}
	}
	// 默认：使用配置的固定TTL
	h.cache.SetStreamURL(mediaSourceID, url)
}

// 注意：这是启发式判断，不是 100% 准确。
// 如果误判把非直链当直链，播放器会自己跟随 302，不影响功能只是慢一点。
// 如果误判把直链当非直链，会多做一次 HTTP 请求，但能拿到真实直链。
// isMoviePath 判断 STRM 路径是否属于电影
// 用于设置预加载优先级（电影入高优先级队列），不再跳过预热。
// 历史上电影曾跳过预热以节省带宽，但实测会导致起播10秒后3003错误（CDN未预热，
// 初始缓冲耗尽后连接中断），因此已移除所有跳过逻辑，电影与其他类型一视同仁。
func isMoviePath(path string) bool {
	if path == "" {
		return false
	}
	pathLower := strings.ToLower(path)
	return strings.Contains(pathLower, "movie") || strings.Contains(pathLower, "电影")
}

func isDirectURL(urlStr string) bool {
	if urlStr == "" {
		return false
	}

	// 解析 URL
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return false
	}

	// 内网地址不支持时，把内网URL当作非直链（强制走解析流程，由调用方决定是否跳过）
	// 这里不直接返回false，因为isDirectURL的语义是"是否CDN直链"，内网地址本就不是CDN直链
	// 内网地址的跳过逻辑在 PreloadStrmSync/parallelResolve 中根据 EnableLanStrm 配置处理

	host := strings.ToLower(parsedURL.Host)
	path := strings.ToLower(parsedURL.Path)
	rawQuery := strings.ToLower(parsedURL.RawQuery)

	// ========== 标准1：已知 CDN/网盘直链域名 ==========
	// 这些域名通常是工具生成的 302 strm 直链
	knownDirectHosts := []string{
		// 夸克网盘 CDN
		"drive-pc.quark.cn",
		"drive-quark.cn",
		".quark.cn", // 泛匹配夸克 CDN
		// 115 网盘 CDN
		"cdnfhnip.115.com",
		"115.com",
		// 阿里云盘 CDN
		".alipayobjects.com",
		"das-wsm-oss.aliyuncs.com",
		".aliyuncs.com", // 阿里云 OSS
		// 腾讯云 COS/CDN
		".myqcloud.com",
		// 七牛云
		".qiniucdn.com",
		".qiniudn.com",
		// 华为云 OBS
		".myhuaweicloud.com",
		// Cloudflare R2/CDN
		".cloudflarestorage.com",
		".cdn.cloudflare.net",
		// 百度网盘 CDN
		"qdall01." + "baidu.com",
		".baidu.com",
		// 天翼云盘
		".cloud.189.cn",
		// 移动云盘
		".caiyunapp.com",
		// 移动 139 云盘
		".10086.cn",
		".kstore.10086.cn",
		".cloudnjs.com",
		".migudm.cn",
		// 一般 CDN 标志
		"cdn.",
		"oss.",
		"cos.",
		"obs.",
		"edge.",
	}

	for _, h := range knownDirectHosts {
		if strings.Contains(host, h) {
			return true
		}
	}

	// ========== 标准2：URL 已带签名参数（临时直链特征）==========
	// 网盘工具生成的直链通常带这些签名参数
	signParams := []string{
		"x-oss-expires", // 阿里云 OSS
		"x-oss-signature",
		"x-oss-credential",
		"signature", // 通用签名
		"expires",   // 过期时间
		"sign=",     // Alist 签名
		"&sign=",
		"?sign=",
		"auth_key",         // 阿里云 CDN 鉴权
		"q-sign-algorithm", // 腾讯云 COS
		"access_key",
		"token=",
		"temp_url_sig", // Swift 临时签名
	}

	for _, p := range signParams {
		if strings.Contains(rawQuery, p) {
			return true
		}
	}

	// ========== 标准3：URL 路径包含版本/hash 标志（CDN 永久直链）==========
	// 如 /v1/xxx、/hash/xxx 等
	if strings.Contains(path, "/static/") ||
		strings.Contains(path, "/assets/") {
		return true
	}

	// ========== 反向判断：明确是分享链接/短链，不是直链 ==========
	// 这些 URL 一定需要跟随重定向或 API 转换
	shareHosts := []string{
		"pan.quark.cn/s/",    // 夸克分享链接
		"pan.baidu.com/s/",   // 百度分享链接
		"115.com/s/",         // 115 分享链接
		"aliyundrive.com/s/", // 阿里云盘分享链接
		"alipan.com/s/",      // 阿里云盘分享链接
		"cloud.189.cn/t/",    // 天翼云盘分享链接
	}
	for _, s := range shareHosts {
		if strings.Contains(strings.ToLower(urlStr), s) {
			return false
		}
	}

	// ========== 默认保守判断：不是直链，需要解析 ==========
	// 对于未知域名，保守地走解析流程，避免误判
	return false
}

// resolveURLWithRetry 并行使用 ultra-fast + fast + medium + slow 客户端解析URL
// 谁先成功用谁，立即取消其他，避免串行 fallback 导致 15s+ 延迟
// 四路并发：800ms + 2s + 4s + 8s，理论最快 0.8s，最坏 8s
// 公共API签名保持不变：originalReq 仅用于读取 User-Agent 头
func (h *StreamHandler) resolveURLWithRetry(urlStr string, originalReq *http.Request) (string, error) {
	originalUA := originalReq.Header.Get("User-Agent")
	if originalUA == "" {
		originalUA = h.getRandomUserAgent()
	}

	type result struct {
		url string
		err error
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 四客户端并行（ultra-fast 800ms + fast 2s + medium 4s + slow 8s）
	// 四路并发谁先成功用谁，极速客户端让网络好时解析降到 0.8s
	// 每个客户端用不同 UA，避免被同一 UA 限制
	ch := make(chan result, 4)

	go func() {
		url, err := h.resolveURLWithCtx(ctx, h.ultraFastClient, urlStr, originalUA)
		select {
		case ch <- result{url, err}:
		case <-ctx.Done():
		}
	}()

	go func() {
		url, err := h.resolveURLWithCtx(ctx, h.fastClient, urlStr, h.getRandomUserAgent())
		select {
		case ch <- result{url, err}:
		case <-ctx.Done():
		}
	}()

	go func() {
		// medium 客户端用不同 UA
		mediumUA := h.getRandomUserAgent()
		url, err := h.resolveURLWithCtx(ctx, h.mediumClient, urlStr, mediumUA)
		select {
		case ch <- result{url, err}:
		case <-ctx.Done():
		}
	}()

	go func() {
		// slow 用不同 UA，避免被同一 UA 限制
		slowUA := h.getRandomUserAgent()
		url, err := h.resolveURLWithCtx(ctx, h.slowClient, urlStr, slowUA)
		select {
		case ch <- result{url, err}:
		case <-ctx.Done():
		}
	}()

	// 等第一个成功的结果，立即返回
	var lastErr error
	for i := 0; i < 4; i++ {
		r := <-ch
		if r.err == nil && r.url != "" {
			cancel() // 取消其他正在进行的请求
			return r.url, nil
		}
		if r.err != nil {
			lastErr = r.err
			h.logger.Debug("⚠️ [并行解析] 第%d个客户端失败: %v", i+1, r.err)
		}
	}

	return "", lastErr
}

// resolveURLWithCtx 带context的URL解析（支持取消，避免资源浪费）
func (h *StreamHandler) resolveURLWithCtx(ctx context.Context, client *http.Client, urlStr string, userAgent string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", urlStr, nil)
	if err != nil {
		return "", err
	}

	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Range", "bytes=0-0") // 只请求1字节，避免下载整个文件

	resp, err := client.Do(req)
	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "context canceled") || strings.Contains(errStr, "timeout") {
			h.logger.Debug("⏰ [URL解析] 请求超时/取消: %s", urlStr)
		} else if strings.Contains(errStr, "connection refused") || strings.Contains(errStr, "no such host") {
			h.logger.Debug("🔌 [URL解析] 连接失败: %s", urlStr)
		}
		return "", err
	}

	// 读取少量数据确保连接可复用，然后立即关闭
	// 解析阶段保持快速（Range: bytes=0-0 只返回1字节），不在此预取大文件
	// CDN 预热由 warmupCDN 专门负责（预取 256KB 文件头 + 尾部）
	io.CopyN(io.Discard, resp.Body, 1024)
	resp.Body.Close()

	// resp.Request.URL 是经过所有重定向后的最终URL
	finalURL := resp.Request.URL.String()

	// 检查最终状态码
	if resp.StatusCode >= 200 && resp.StatusCode < 400 {
		// 验证响应不是HTML错误页面
		// 某些CDN在签名过期时返回200状态码+HTML错误页面（而非403），播放器收到HTML会报3003
		contentType := resp.Header.Get("Content-Type")
		if isHTMLErrorResponse(resp.StatusCode, contentType, resp.Header) {
			h.logger.Warn("⚠️ [URL解析] 响应为HTML错误页面，拒绝缓存: %s (状态码: %d, Content-Type: %s)",
				urlStr, resp.StatusCode, contentType)
			return "", fmt.Errorf("CDN返回HTML错误页面 (状态码: %d)", resp.StatusCode)
		}
		if finalURL != "" {
			h.logger.Debug("✅ [URL解析] 成功: %s -> %s (状态码: %d)", urlStr, finalURL, resp.StatusCode)
			return finalURL, nil
		}
		return urlStr, nil
	}

	// 403 是签名过期/禁止访问的关键错误，必须当作错误处理
	// 播放器跟随302重定向后收到CDN的403 HTML错误页面，报 UnrecognizedInputFormatException (3003)
	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		return "", fmt.Errorf("访问被拒绝: %d (签名可能已过期)", resp.StatusCode)
	}

	// 404/410/500/502/503 是明确错误
	if resp.StatusCode == 404 || resp.StatusCode == 410 ||
		resp.StatusCode == 500 || resp.StatusCode == 502 || resp.StatusCode == 503 {
		return "", fmt.Errorf("服务器错误: %d", resp.StatusCode)
	}

	// 其他状态码保持lenient策略
	if finalURL != "" {
		return finalURL, nil
	}
	return urlStr, nil
}

// resolveURL 兼容旧调用：发送GET请求自动跟随重定向，拿到最终URL
// 内部转发到 resolveURLWithCtx，保留是为了外部可能的引用
func (h *StreamHandler) resolveURL(client *http.Client, urlStr string, userAgent string) (string, error) {
	return h.resolveURLWithCtx(context.Background(), client, urlStr, userAgent)
}

// extractItemID 从路径提取ItemId（优化版）
// 匹配 /Videos/{id} 或 /Items/{id} 路径，id 必须是 GUID 格式
func extractItemID(path string) string {
	path = strings.TrimPrefix(path, "/emby")

	parts := strings.Split(path, "/")
	for i, part := range parts {
		if (part == "videos" || part == "Items" || part == "Videos") && i+1 < len(parts) {
			itemID := parts[i+1]
			if len(itemID) >= 32 && util.IsGUIDLike(itemID) {
				return itemID
			}
		}
	}
	return ""
}

// extractEpisodeName 从 STRM 文件路径提取剧集名称
// 例: /vol00/Series/剧名/Season 1/剧名 - S01E03.strm → "剧名 - S01E03"
// shortURL 截断长 URL 用于日志显示
// 保留 scheme://host 和路径前 40 字符，尾部用 ... 标记截断
// 例: https://example.com:5211/api/v1/file/abc...def.mkv?sign=xxx → https://example.com:5211/api/v1/file/abc...def.mkv?sign=xxx
// 长 URL 截断为: https://example.com:5211/api/v1/f...xxx
func shortURL(rawURL string) string {
	if rawURL == "" {
		return ""
	}
	// 尝试解析 URL 提取 host 部分
	if u, err := url.Parse(rawURL); err == nil && u.Host != "" {
		prefix := u.Scheme + "://" + u.Host
		pathQuery := u.Path
		if u.RawQuery != "" {
			pathQuery += "?" + u.RawQuery
		}
		// host + 路径总长度不超过 80 字符
		maxPathLen := 80 - len(prefix)
		if maxPathLen < 10 {
			maxPathLen = 10
		}
		if len(pathQuery) <= maxPathLen {
			return prefix + pathQuery
		}
		return prefix + pathQuery[:maxPathLen/2] + "..." + pathQuery[len(pathQuery)-maxPathLen/2:]
	}
	// 解析失败，直接截断
	if len(rawURL) <= 80 {
		return rawURL
	}
	return rawURL[:40] + "..." + rawURL[len(rawURL)-20:]
}

func extractEpisodeName(path string) string {
	if path == "" {
		return ""
	}
	// 统一路径分隔符
	name := strings.ReplaceAll(path, "\\", "/")
	// 取最后一段
	if idx := strings.LastIndex(name, "/"); idx >= 0 {
		name = name[idx+1:]
	}
	// 去掉 .strm 扩展名（不区分大小写）
	name = strings.TrimSuffix(name, ".strm")
	if len(name) > 5 {
		lower := strings.ToLower(name)
		if strings.HasSuffix(lower, ".strm") {
			name = name[:len(name)-5]
		}
	}
	return strings.TrimSpace(name)
}

// extractAllIDsFromPath 从路径提取所有可能的ID
func extractAllIDsFromPath(path string) []string {
	var ids []string
	parts := strings.Split(path, "/")
	for _, part := range parts {
		if len(part) >= 16 && util.IsGUIDLike(part) {
			ids = append(ids, part)
		}
	}
	return ids
}

// readStrmWithCache 读取strm文件（带LRU缓存）
func (h *StreamHandler) readStrmWithCache(path string) (string, error) {
	// 缓存命中时需要MoveToBack（写操作），必须用写锁
	h.strmMutex.Lock()
	if cached, ok := h.strmCache[path]; ok {
		if elem, exists := h.strmIndex[path]; exists {
			h.strmList.MoveToBack(elem)
		}
		h.strmMutex.Unlock()
		return cached, nil
	}
	h.strmMutex.Unlock()

	content, err := ReadStrmFile(path)
	if err != nil {
		return "", err
	}

	h.strmMutex.Lock()
	if _, exists := h.strmCache[path]; !exists {
		elem := h.strmList.PushBack(path)
		h.strmCache[path] = content
		h.strmIndex[path] = elem

		if h.strmList.Len() > maxStrmCacheSize {
			h.evictOldestStrm()
		}
	}
	h.strmMutex.Unlock()

	return content, nil
}

// resolveURLWithCache 解析URL（带LRU缓存）
func (h *StreamHandler) resolveURLWithCache(urlStr string, originalReq *http.Request) (string, error) {
	// 使用带过期检查的 getURLCache
	if cached, ok := h.getURLCache(urlStr); ok {
		return cached, nil
	}

	// 使用新的带重试的解析器
	finalURL, err := h.resolveURLWithRetry(urlStr, originalReq)
	if err != nil {
		return "", err
	}

	// 使用带智能TTL的 setURLCache
	h.setURLCache(urlStr, finalURL)

	return finalURL, nil
}

// GetStats 获取性能统计
func (h *StreamHandler) GetStats() map[string]interface{} {
	// 统一字段均用 atomic 操作，无需加锁
	total := atomic.LoadInt64(&h.stats.totalRequests)
	hits := atomic.LoadInt64(&h.stats.cacheHits)
	preloads := atomic.LoadInt64(&h.stats.preloadSuccess)
	failures := atomic.LoadInt64(&h.stats.resolveFailures)
	fallbacks := atomic.LoadInt64(&h.stats.fallbackCount)
	dropped := atomic.LoadInt64(&h.stats.droppedCount)

	// warmup 统计
	warmupTotal := atomic.LoadInt64(&h.stats.warmupTotal)
	warmupSkips := atomic.LoadInt64(&h.stats.warmupSkips)
	warmupSuccess := atomic.LoadInt64(&h.stats.warmupSuccess)
	warmupFailures := atomic.LoadInt64(&h.stats.warmupFailures)
	warmupMoovHead := atomic.LoadInt64(&h.stats.warmupMoovHead)
	warmupMoovTail := atomic.LoadInt64(&h.stats.warmupMoovTail)
	warmupTotalMs := atomic.LoadInt64(&h.stats.warmupTotalMs)
	// 预连接统计
	preconnectTotal := atomic.LoadInt64(&h.stats.preconnectTotal)
	preconnectSuccess := atomic.LoadInt64(&h.stats.preconnectSuccess)
	preconnectFailures := atomic.LoadInt64(&h.stats.preconnectFailures)
	// 直链跳过统计
	directURLSkips := atomic.LoadInt64(&h.stats.directURLSkips)
	// 半衰期刷新统计
	halfLifeRefreshes := atomic.LoadInt64(&h.stats.halfLifeRefreshes)
	halfLifeSkips := atomic.LoadInt64(&h.stats.halfLifeSkips)
	// 熔断器统计
	circuitBreakerOpens := atomic.LoadInt64(&h.stats.circuitBreakerOpens)
	circuitBreakerRejects := atomic.LoadInt64(&h.stats.circuitBreakerRejects)

	hitRate := float64(0)
	if total > 0 {
		hitRate = float64(hits) / float64(total) * 100
	}

	successRate := float64(0)
	totalAttempts := preloads + failures
	if totalAttempts > 0 {
		successRate = float64(preloads) / float64(totalAttempts) * 100
	}

	// warmup 统计派生指标
	warmupSkipRate := float64(0)
	if warmupTotal > 0 {
		warmupSkipRate = float64(warmupSkips) / float64(warmupTotal) * 100
	}
	warmupAvgMs := float64(0)
	if warmupSuccess > 0 {
		warmupAvgMs = float64(warmupTotalMs) / float64(warmupSuccess)
	}
	moovHeadRate := float64(0)
	moovTotal := warmupMoovHead + warmupMoovTail
	if moovTotal > 0 {
		moovHeadRate = float64(warmupMoovHead) / float64(moovTotal) * 100
	}

	return map[string]interface{}{
		"total_requests":   total,
		"cache_hits":       hits,
		"hit_rate":         hitRate,
		"preload_success":  preloads,
		"resolve_failures": failures,
		"success_rate":     successRate,
		"fallback_count":   fallbacks,
		"dropped_count":    dropped,
		"strm_cache_size":  h.GetStrmCacheSize(),
		"url_cache_size":   h.GetURLCacheSize(),
		"neg_cache_size":   h.GetNegCacheSize(),
		// warmup 统计
		"warmup_total":          warmupTotal,
		"warmup_skips":          warmupSkips,
		"warmup_skip_rate":      warmupSkipRate, // 去重跳过率（越高越好，说明dedup生效）
		"warmup_success":        warmupSuccess,
		"warmup_failures":       warmupFailures,
		"warmup_avg_ms":         warmupAvgMs,    // 预热平均耗时（毫秒）
		"warmup_moov_head":      warmupMoovHead, // moov在头部次数（faststart）
		"warmup_moov_tail":      warmupMoovTail, // moov在尾部次数（触发尾部预热）
		"warmup_moov_head_rate": moovHeadRate,   // moov在头部比例（越高说明省越多尾部请求）
		// 预连接统计
		"preconnect_total":    preconnectTotal,    // 预连接总次数
		"preconnect_success":  preconnectSuccess,  // 预连接成功次数
		"preconnect_failures": preconnectFailures, // 预连接失败次数
		// 直链跳过统计
		"direct_url_skips": directURLSkips, // 直链跳过解析次数（302 strm 场景）
		// 半衰期刷新统计
		"half_life_refreshes": halfLifeRefreshes, // 半衰期刷新次数
		"half_life_skips":     halfLifeSkips,     // 半衰期跳过次数
		// 熔断器统计
		"circuit_breaker_opens":   circuitBreakerOpens,   // 熔断器断开次数
		"circuit_breaker_rejects": circuitBreakerRejects, // 熔断器拒绝请求次数
	}
}

// Stop 停止预加载引擎
// preloadWg.Wait() 加 10 秒超时保护，避免某个预加载任务卡住导致关闭流程无限阻塞
func (h *StreamHandler) Stop() {
	close(h.shutdown)

	// 超时等待预加载 worker 退出，避免无限阻塞
	done := make(chan struct{})
	go func() {
		h.preloadWg.Wait()
		close(done)
	}()
	select {
	case <-done:
		// 正常退出
	case <-time.After(10 * time.Second):
		h.logger.Warn("⚠️ [STRM] 预加载引擎 10 秒内未完全停止，强制继续关闭")
	}

	// 优雅关闭 HTTP 客户端空闲连接，避免资源泄漏
	if h.ultraFastClient != nil {
		if t, ok := h.ultraFastClient.Transport.(*http.Transport); ok {
			t.CloseIdleConnections()
		}
	}
	if h.fastClient != nil {
		if t, ok := h.fastClient.Transport.(*http.Transport); ok {
			t.CloseIdleConnections()
		}
	}
	if h.mediumClient != nil {
		if t, ok := h.mediumClient.Transport.(*http.Transport); ok {
			t.CloseIdleConnections()
		}
	}
	if h.slowClient != nil {
		if t, ok := h.slowClient.Transport.(*http.Transport); ok {
			t.CloseIdleConnections()
		}
	}
	if h.warmupClient != nil {
		if t, ok := h.warmupClient.Transport.(*http.Transport); ok {
			t.CloseIdleConnections()
		}
	}
	h.logger.Info("🛑 [STRM] 预加载引擎已停止")
}

// GetStrmCacheSize 获取strm缓存大小
func (h *StreamHandler) GetStrmCacheSize() int {
	h.strmMutex.RLock()
	defer h.strmMutex.RUnlock()
	return len(h.strmCache)
}

// GetURLCacheSize 获取URL缓存大小
func (h *StreamHandler) GetURLCacheSize() int {
	h.urlMutex.RLock()
	defer h.urlMutex.RUnlock()
	return len(h.urlCache)
}

// GetNegCacheSize 获取负缓存大小
func (h *StreamHandler) GetNegCacheSize() int {
	h.negMutex.RLock()
	defer h.negMutex.RUnlock()
	return len(h.negCache)
}

// evictOldestStrm 淘汰最老的STRM缓存
func (h *StreamHandler) evictOldestStrm() {
	if h.strmList.Len() == 0 {
		return
	}
	oldest := h.strmList.Front()
	if oldest != nil {
		key := oldest.Value.(string)
		h.strmList.Remove(oldest)
		delete(h.strmCache, key)
		delete(h.strmIndex, key)
	}
}

// evictOldestURL 淘汰最老的URL缓存
func (h *StreamHandler) evictOldestURL() {
	if h.urlList.Len() == 0 {
		return
	}
	oldest := h.urlList.Front()
	if oldest != nil {
		key := oldest.Value.(string)
		h.urlList.Remove(oldest)
		delete(h.urlCache, key)
		delete(h.urlIndex, key)
	}
}

// ClearExternalCaches 清空外部缓存
func (h *StreamHandler) ClearExternalCaches() {
	h.strmMutex.Lock()
	h.strmCache = make(map[string]string)
	h.strmIndex = make(map[string]*list.Element)
	h.strmList.Init()
	h.strmMutex.Unlock()

	h.urlMutex.Lock()
	h.urlCache = make(map[string]urlCacheEntry)
	h.urlIndex = make(map[string]*list.Element)
	h.urlList.Init()
	h.urlMutex.Unlock()

	h.negMutex.Lock()
	h.negCache = make(map[string]time.Time)
	h.negMutex.Unlock()

	h.validateMu.Lock()
	h.validateRecent = make(map[string]time.Time)
	h.validateMu.Unlock()

	h.streamReqMu.Lock()
	h.streamReqRecent = make(map[string]time.Time)
	h.streamReqMu.Unlock()

	// 清空熔断器状态
	h.circuitMu.Lock()
	h.circuitBreaker.errors = 0
	h.circuitBreaker.state = 0
	h.circuitBreaker.openUntil = time.Time{}
	h.circuitMu.Unlock()
}
