package cache

import (
	"container/list"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// streamPrefix 直链缓存在 LRU 链表中使用的前缀，加长且加双下划线避免与真实 MediaSourceId 冲突
const streamPrefix = "__stream_cache__"

// MediaSource 媒体源信息
type MediaSource struct {
	ID       string
	ItemID   string // 关联的ItemId
	Path     string
	Protocol string
	Name     string // 剧集名称（用于日志显示）
}

// StreamURL 视频流直链缓存
type StreamURL struct {
	URL        string
	MediaSrcID string // 关联的 MediaSourceId
}

// cacheItem 带过期时间的缓存项
type cacheItem struct {
	source  MediaSource
	expire  time.Time
	element *list.Element // LRU链表指针
}

// streamCacheItem 带过期时间的直链缓存项
type streamCacheItem struct {
	url       StreamURL
	expire    time.Time
	element   *list.Element // LRU链表指针
	createdAt time.Time     // 记录入缓存时间，用于计算正确的 stale 百分比（修复误用全局 TTL）
}

// singleFlight 用于防止缓存击穿
type singleFlight struct {
	mu       sync.Mutex
	inFlight map[string]*call
}

type call struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

func (sf *singleFlight) do(key string, fn func() (interface{}, error)) (interface{}, error) {
	sf.mu.Lock()
	if sf.inFlight == nil {
		sf.inFlight = make(map[string]*call)
	}
	if c, ok := sf.inFlight[key]; ok {
		sf.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}
	c := new(call)
	c.wg.Add(1)
	sf.inFlight[key] = c
	sf.mu.Unlock()

	c.val, c.err = fn()

	c.wg.Done()

	sf.mu.Lock()
	delete(sf.inFlight, key)
	sf.mu.Unlock()

	return c.val, c.err
}

// Stats 缓存统计信息
type Stats struct {
	MediaSourceCount int     `json:"media_source_count"` // MediaSource缓存数量
	StreamURLCount   int     `json:"stream_url_count"`   // 直链缓存数量
	StrmCacheCount   int     `json:"strm_cache_count"`   // strm文件缓存数量（外部）
	URLCacheCount    int     `json:"url_cache_count"`    // URL解析缓存数量（外部）
	MemoryUsageMB    float64 `json:"memory_usage_mb"`    // 估算内存使用(MB)
	HitCount         int64   `json:"hit_count"`          // 命中次数
	MissCount        int64   `json:"miss_count"`         // 未命中次数
	HitRate          float64 `json:"hit_rate"`           // 命中率
	EvictedCount     int64   `json:"evicted_count"`      // 淘汰次数
}

// Cache 缓存（带LRU和大小限制）
type Cache struct {
	items       map[string]cacheItem       // MediaSourceId -> MediaSource
	itemIndex   map[string]string          // ItemId -> MediaSourceId
	streamURLs  map[string]streamCacheItem // MediaSourceId -> StreamURL
	lruList     *list.List                 // LRU双向链表
	mutex       sync.RWMutex
	ttl         time.Duration
	maxItems    int // 最大缓存条目数（0=无限制）
	stopCleaner chan struct{}
	flight      singleFlight

	stats         Stats        // 统计信息
	statsMutex    sync.RWMutex // 统计锁
	evictedCount  atomic.Int64 // 淘汰次数（atomic，避免与 c.mutex 嵌套加锁）
	cleanerClosed atomic.Bool  // 当前 stopCleaner 是否已关闭（防 close panic）
	stopped       atomic.Bool  // cache 是否已 Stop（Stop 后不再重启 cleaner）
}

const (
	defaultMaxItems = 10000 // 默认最大缓存条目数
)

// New 创建缓存，默认1小时过期
func New() *Cache {
	return NewWithTTL(1 * time.Hour)
}

// NewWithStreamTTL 创建缓存，指定直链 TTL
func NewWithStreamTTL(streamTTL time.Duration) *Cache {
	return NewWithLimits(streamTTL, defaultMaxItems)
}

// NewWithTTL 创建指定过期时间的缓存（兼容旧接口）
func NewWithTTL(ttl time.Duration) *Cache {
	return NewWithLimits(ttl, defaultMaxItems)
}

// NewWithLimits 创建带大小限制的缓存
func NewWithLimits(ttl time.Duration, maxItems int) *Cache {
	if maxItems <= 0 {
		maxItems = defaultMaxItems
	}

	c := &Cache{
		items:       make(map[string]cacheItem),
		itemIndex:   make(map[string]string),
		streamURLs:  make(map[string]streamCacheItem),
		lruList:     list.New(),
		ttl:         ttl,
		maxItems:    maxItems,
		stopCleaner: make(chan struct{}),
	}
	go c.cleaner()
	return c
}

// Set 设置 MediaSource 缓存
func (c *Cache) Set(id string, source MediaSource) {
	c.mutex.Lock()

	if existing, exists := c.items[id]; exists && existing.element != nil {
		c.lruList.MoveToBack(existing.element)
	} else {
		element := c.lruList.PushBack(id)
		c.items[id] = cacheItem{
			source:  source,
			expire:  time.Now().Add(c.ttl),
			element: element,
		}
	}

	if source.ItemID != "" {
		c.itemIndex[source.ItemID] = id
	}

	if c.maxItems > 0 && len(c.items) > c.maxItems {
		c.evictOldest()
	}

	c.mutex.Unlock()
}

// Get 获取缓存（自动检查过期 + 更新LRU）
func (c *Cache) Get(id string) (MediaSource, bool) {
	c.mutex.Lock()
	item, found := c.items[id]
	if !found {
		c.mutex.Unlock()
		c.recordMiss()
		return MediaSource{}, false
	}

	if time.Now().After(item.expire) {
		c.removeItemLocked(id)
		c.mutex.Unlock()
		c.recordMiss()
		return MediaSource{}, false
	}

	if item.element != nil {
		c.lruList.MoveToBack(item.element)
	}

	source := item.source
	c.mutex.Unlock()
	c.recordHit()
	return source, true
}

// GetOrLoad 获取缓存，未命中时通过 singleFlight 调用 loader 加载并缓存（防止缓存击穿）
// 注意：loader 内部不应再次调用本 Cache 的 Set/Get 同一 id，避免自旋
func (c *Cache) GetOrLoad(id string, loader func() (MediaSource, error)) (MediaSource, error) {
	// 快速路径：命中直接返回
	if src, found := c.Get(id); found {
		return src, nil
	}
	// 未命中，用 singleFlight 合并并发加载
	val, err := c.flight.do(id, func() (interface{}, error) {
		// double-check：其他协程可能已加载并写入缓存
		if src, found := c.Get(id); found {
			return src, nil
		}
		src, err := loader()
		if err != nil {
			return MediaSource{}, err
		}
		c.Set(id, src)
		return src, nil
	})
	if err != nil {
		return MediaSource{}, err
	}
	return val.(MediaSource), nil
}

// GetByItemID 通过ItemId查找MediaSource
// 注意：查找与读取必须在同一次 Lock 内完成，避免两次加锁之间 id 被 Delete 导致索引失效
func (c *Cache) GetByItemID(itemID string) (MediaSource, bool) {
	c.mutex.Lock()

	mediaSourceID, found := c.itemIndex[itemID]
	if !found {
		c.mutex.Unlock()
		c.recordMiss()
		return MediaSource{}, false
	}

	item, foundItem := c.items[mediaSourceID]
	if !foundItem {
		// 索引指向了已不存在的条目，清理脏索引
		delete(c.itemIndex, itemID)
		c.mutex.Unlock()
		c.recordMiss()
		return MediaSource{}, false
	}

	if time.Now().After(item.expire) {
		c.removeItemLocked(mediaSourceID)
		c.mutex.Unlock()
		c.recordMiss()
		return MediaSource{}, false
	}

	if item.element != nil {
		c.lruList.MoveToBack(item.element)
	}

	source := item.source
	c.mutex.Unlock()
	c.recordHit()
	return source, true
}

// Delete 删除缓存
func (c *Cache) Delete(id string) {
	c.mutex.Lock()
	c.removeItemLocked(id)
	c.mutex.Unlock()
}

// removeItemLocked 内部删除方法（需要持有锁）
// id 可能为普通 MediaSourceId，也可能为 streamPrefix+id 形式的直链条目
func (c *Cache) removeItemLocked(id string) {
	// 处理 stream 前缀的直链缓存
	if strings.HasPrefix(id, streamPrefix) {
		realID := strings.TrimPrefix(id, streamPrefix)
		if item, ok := c.streamURLs[realID]; ok {
			if item.element != nil {
				c.lruList.Remove(item.element)
			}
			delete(c.streamURLs, realID)
		}
		return
	}
	// 普通 MediaSource 缓存
	if item, ok := c.items[id]; ok {
		if item.element != nil {
			c.lruList.Remove(item.element)
		}
		if item.source.ItemID != "" {
			delete(c.itemIndex, item.source.ItemID)
		}
		delete(c.items, id)
	}
}

// evictOldest 淘汰最老的缓存项
func (c *Cache) evictOldest() {
	if c.lruList.Len() == 0 {
		return
	}

	oldest := c.lruList.Front()
	if oldest != nil {
		id := oldest.Value.(string)
		c.removeItemLocked(id)
		// 用 atomic 自增，避免在持有 c.mutex 期间再获取 statsMutex 造成嵌套锁
		c.evictedCount.Add(1)
	}
}

// SetStreamURL 设置直链缓存
func (c *Cache) SetStreamURL(mediaSourceID string, url string) {
	c.SetStreamURLWithTTL(mediaSourceID, url, c.ttl)
}

// SetStreamURLWithTTL 设置直链缓存（带自定义TTL）
// 用于智能签名TTL：根据URL签名有效期动态设置缓存过期时间
// 例如：URL签名有效期1小时，传入30分钟TTL（取50%安全余量）
// 修复：已存在条目现在会刷新 URL 和过期时间
func (c *Cache) SetStreamURLWithTTL(mediaSourceID string, url string, ttl time.Duration) {
	c.mutex.Lock()
	now := time.Now()

	var element *list.Element
	if existing, exists := c.streamURLs[mediaSourceID]; exists && existing.element != nil {
		// 复用已有的 LRU 节点，移动到队尾
		element = existing.element
		c.lruList.MoveToBack(element)
	} else {
		element = c.lruList.PushBack(streamPrefix + mediaSourceID)
	}

	// 无论新建还是已存在，都更新 URL、过期时间和创建时间
	c.streamURLs[mediaSourceID] = streamCacheItem{
		url: StreamURL{
			URL:        url,
			MediaSrcID: mediaSourceID,
		},
		expire:    now.Add(ttl),
		element:   element,
		createdAt: now,
	}

	if c.maxItems > 0 && (len(c.items)+len(c.streamURLs)) > c.maxItems {
		c.evictOldest()
	}

	c.mutex.Unlock()
}

// GetStreamURL 获取直链缓存
// 返回值约定：(url, true) 表示新鲜可用；(StreamURL{}, false) 表示未命中/已过期；
// (url, false) 表示命中但剩余 TTL 不足 10%（即将过期），调用方应重新解析。
func (c *Cache) GetStreamURL(mediaSourceID string) (StreamURL, bool) {
	c.mutex.Lock()
	item, found := c.streamURLs[mediaSourceID]
	if !found {
		c.mutex.Unlock()
		c.recordMiss()
		return StreamURL{}, false
	}

	now := time.Now()
	if now.After(item.expire) {
		if item.element != nil {
			c.lruList.Remove(item.element)
		}
		delete(c.streamURLs, mediaSourceID)
		c.mutex.Unlock()
		c.recordMiss()
		return StreamURL{}, false
	}

	if item.element != nil {
		c.lruList.MoveToBack(item.element)
	}

	url := item.url
	// 链接过期感知：剩余 TTL 小于条目原始 TTL 的 10% 时，返回 false 提示调用方刷新
	// 修复：误用全局 c.ttl 判断 stale，当条目使用智能短 TTL（如 5 分钟）而全局 TTL 为 30 分钟时，
	// stale 判断完全失效（remaining*10 < 30min 意味着 remaining < 3min，5 分钟 TTL 的条目在第 2 分钟就被标记 stale）
	// 现在改用条目自身的原始 TTL（expire - createdAt）计算正确的 10% 阈值
	stale := false
	originalTTL := item.expire.Sub(item.createdAt)
	if originalTTL > 0 {
		remaining := item.expire.Sub(now)
		if remaining*10 < originalTTL {
			stale = true
		}
	}
	c.mutex.Unlock()

	c.recordHit()
	if stale {
		// 即将过期：返回 URL 但 false，让调用方重新解析（URL 仍可作 fallback）
		return url, false
	}
	return url, true
}

// DeleteStreamURL 删除直链缓存
func (c *Cache) DeleteStreamURL(mediaSourceID string) {
	c.mutex.Lock()
	if item, ok := c.streamURLs[mediaSourceID]; ok {
		if item.element != nil {
			c.lruList.Remove(item.element)
		}
		delete(c.streamURLs, mediaSourceID)
	}
	c.mutex.Unlock()
}

// Clear 清空所有缓存
func (c *Cache) Clear() {
	c.mutex.Lock()
	c.items = make(map[string]cacheItem)
	c.itemIndex = make(map[string]string)
	c.streamURLs = make(map[string]streamCacheItem)
	c.lruList.Init()
	c.mutex.Unlock()

	c.statsMutex.Lock()
	c.stats.HitCount = 0
	c.stats.MissCount = 0
	c.statsMutex.Unlock()
	// EvictedCount 已改为 atomic 存储，单独重置
	c.evictedCount.Store(0)
}

// cleanup 定期清理过期缓存
func (c *Cache) cleanup() {
	now := time.Now()
	c.mutex.Lock()

	for id, item := range c.items {
		if now.After(item.expire) {
			c.removeItemLocked(id)
		}
	}

	for id, item := range c.streamURLs {
		if now.After(item.expire) {
			if item.element != nil {
				c.lruList.Remove(item.element)
			}
			delete(c.streamURLs, id)
		}
	}

	c.mutex.Unlock()
}

// cleaner 后台清理协程
func (c *Cache) cleaner() {
	ticker := time.NewTicker(c.ttl / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.cleanup()
		case <-c.stopCleaner:
			return
		}
	}
}

// Stop 停止清理协程
// 多次调用安全：通过 stopped/cleanerClosed 两个 atomic 标记防止 close of closed channel panic
func (c *Cache) Stop() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// 标记整个 cache 已停止，UpdateTTL 不再重启 cleaner
	if c.stopped.Swap(true) {
		return
	}
	// 关闭当前 stopCleaner（若尚未关闭）
	if !c.cleanerClosed.Swap(true) {
		close(c.stopCleaner)
	}
}

// UpdateTTL 动态更新缓存TTL
func (c *Cache) UpdateTTL(newTTL time.Duration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.ttl == newTTL {
		return
	}

	c.ttl = newTTL

	// Stop 之后不再重启 cleaner，避免 goroutine 泄漏与 close panic
	if c.stopped.Load() {
		return
	}

	// 关闭旧 cleaner 通道以停止旧协程，避免 goroutine 泄漏
	// 用 cleanerClosed atomic 防 close of closed channel
	if !c.cleanerClosed.Swap(true) {
		close(c.stopCleaner)
	}
	// 重建 cleaner，新协程使用新 TTL 间隔
	c.stopCleaner = make(chan struct{})
	c.cleanerClosed.Store(false)
	go c.cleaner()
}

// UpdateMaxItems 动态更新最大缓存条目数
func (c *Cache) UpdateMaxItems(newMax int) {
	if newMax <= 0 {
		return
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	oldMax := c.maxItems
	c.maxItems = newMax

	if newMax < oldMax && len(c.items) > newMax {
		for c.lruList.Len() > newMax {
			c.evictOldest()
		}
	}
}

// GetStats 获取缓存统计信息
func (c *Cache) GetStats() Stats {
	c.statsMutex.RLock()
	stats := c.stats
	c.statsMutex.RUnlock()
	// EvictedCount 改为 atomic 存储，单独读取
	stats.EvictedCount = c.evictedCount.Load()

	c.mutex.RLock()
	stats.MediaSourceCount = len(c.items)
	stats.StreamURLCount = len(c.streamURLs)
	c.mutex.RUnlock()

	total := stats.HitCount + stats.MissCount
	if total > 0 {
		stats.HitRate = float64(stats.HitCount) / float64(total) * 100
	}

	// 估算内存：MediaSource 含 ID/ItemID/Path/Protocol 四个字符串，按平均 Path 长度 256 + 其他字段与结构开销 144 估算
	estimatedSize := (stats.MediaSourceCount + stats.StreamURLCount) * (256 + 144)
	stats.MemoryUsageMB = float64(estimatedSize) / 1024 / 1024

	return stats
}

// recordHit 记录命中
func (c *Cache) recordHit() {
	c.statsMutex.Lock()
	c.stats.HitCount++
	c.statsMutex.Unlock()
}

// recordMiss 记录未命中
func (c *Cache) recordMiss() {
	c.statsMutex.Lock()
	c.stats.MissCount++
	c.statsMutex.Unlock()
}

// SetExternalCacheCounts 设置外部缓存计数（strm/url缓存）
func (c *Cache) SetExternalCacheCounts(strmCount, urlCount int) {
	c.statsMutex.Lock()
	c.stats.StrmCacheCount = strmCount
	c.stats.URLCacheCount = urlCount
	c.statsMutex.Unlock()
}
