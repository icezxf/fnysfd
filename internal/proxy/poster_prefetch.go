
package proxy

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"fnysfd/internal/config"
	"fnysfd/internal/logger"
	"fnysfd/internal/util"
)

// PosterPrefetcher 海报墙预取器
//
// 支持两种 API 风格：
// 1. Emby 兼容 API（爆米花/Vidhub/Infuse）→ 精确预取
// 2. FNOS 原生 API（飞牛 Web/客户端）→ 触发单库扫描
type PosterPrefetcher struct {
	server    *Server
	batch     *BatchPrefetcher
	logger    *logger.Logger
	authStore *AuthStore

	// 媒体库映射缓存
	mu       sync.RWMutex
	fnosLibs map[string]string // FNOS guid → title
	embyLibs map[string]string // Emby name → libId
}

func NewPosterPrefetcher(s *Server, b *BatchPrefetcher, auth *AuthStore) *PosterPrefetcher {
	return &PosterPrefetcher{
		server:    s,
		batch:     b,
		logger:    s.logger,
		authStore: auth,
		fnosLibs:  make(map[string]string),
		embyLibs:  make(map[string]string),
	}
}

// ============================================================
// 媒体库列表缓存
// ============================================================

// CacheEmbyLibraries 拦截 /emby/Users/{uid}/Views 响应时调用
func (p *PosterPrefetcher) CacheEmbyLibraries(body []byte) {
	var resp struct {
		Items []struct {
			Id   string `json:"Id"`
			Name string `json:"Name"`
		} `json:"Items"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, item := range resp.Items {
		if item.Id != "" && item.Name != "" {
			p.embyLibs[item.Name] = item.Id
		}
	}
	p.logger.Debug("🖼️ [海报墙预取] 缓存 Emby 媒体库: %d 个", len(p.embyLibs))
}

// CacheFnosLibraries 拦截 /v/api/v1/mediadb/list 响应时调用
func (p *PosterPrefetcher) CacheFnosLibraries(body []byte) {
	var resp struct {
		Code int `json:"code"`
		Data []struct {
			GUID  string `json:"guid"`
			Title string `json:"title"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, item := range resp.Data {
		if item.GUID != "" && item.Title != "" {
			p.fnosLibs[item.GUID] = item.Title
		}
	}
	p.logger.Debug("🖼️ [海报墙预取] 缓存 FNOS 媒体库: %d 个", len(p.fnosLibs))
}

// mapFnosToEmby 通过名称匹配 FNOS guid → Emby libId
func (p *PosterPrefetcher) mapFnosToEmby(fnosGUID string) string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	title := p.fnosLibs[fnosGUID]
	if title == "" {
		return ""
	}
	return p.embyLibs[title]
}

// ============================================================
// FNOS 原生请求处理
// ============================================================

// HandleFnosListRequest 处理 FNOS 原生海报墙请求
//
// ✅ 终极修复：既然 FNOS 的 API 需要复杂签名，我们直接放弃映射！
// 飞牛客户端请求海报墙时，必然属于某个库，我们直接触发所有 Emby 库的扫描。
func (p *PosterPrefetcher) HandleFnosListRequest(reqBody []byte) {
	if !config.Global.GetEnablePosterPrefetch() {
		return
	}

	// 120 秒全局去重，避免飞牛客户端频繁请求导致重复扫描
	key := "fnos-poster-trigger-global"
	if p.server.shouldSkipPrefetch(key, 120, "FNOS海报墙触发") {
		return
	}

	p.logger.Info("🖼️ [海报墙预取] 捕获 FNOS 海报墙请求，跳过 FNOS 映射，直接触发 Emby 全库预取")

	// 直接从缓存中获取所有 Emby 媒体库
	p.mu.RLock()
	embyLibs := make(map[string]string, len(p.embyLibs))
	for k, v := range p.embyLibs {
		embyLibs[k] = v
	}
	p.mu.RUnlock()

	if len(embyLibs) == 0 {
		p.logger.Warn("🖼️ [海报墙预取] Emby 媒体库缓存为空，无法触发预取")
		return
	}

	// 遍历所有 Emby 库，触发扫描
	if p.server.libraryScanner != nil {
		for name, libID := range embyLibs {
			p.logger.Info("🖼️ [海报墙预取] 触发 Emby 库扫描: %s (%s)", name, libID)
			go func(id string) {
				_ = p.server.libraryScanner.ScanLibraryOnce(context.Background(), id)
			}(libID)
		}
	}
}

// refreshMapping 主动刷新媒体库映射表
func (p *PosterPrefetcher) refreshMapping() bool {
	authHeaders, userID, expired := p.authStore.Get()
	if expired || authHeaders == nil || userID == "" {
		p.logger.Debug("🖼️ [海报墙预取] 刷新映射失败：认证未就绪")
		return false
	}

	// ✅ 核心修复：不管刚才飞牛客户端发了什么乱七八糟的 Token，
	// 这里强制洗回全库扫描用的那个长效 Token！
	authHeaders = p.forceUseLoginToken(authHeaders)

	fnosLibs, err := p.fetchFnosLibraries(authHeaders)
	if err != nil {
		p.logger.Debug("🖼️ [海报墙预取] 拉取 FNOS 媒体库失败: %v", err)
		return false
	}

	embyLibs, err := p.fetchEmbyLibraries(authHeaders, userID)
	if err != nil {
		p.logger.Debug("🖼️ [海报墙预取] 拉取 Emby 媒体库失败: %v", err)
		return false
	}

	p.mu.Lock()
	p.fnosLibs = fnosLibs
	p.embyLibs = embyLibs
	p.mu.Unlock()

	p.logger.Info("🖼️ [海报墙预取] 映射表已刷新: FNOS %d 个, Emby %d 个", len(fnosLibs), len(embyLibs))
	return len(fnosLibs) > 0 && len(embyLibs) > 0
}

// fetchFnosLibraries 拉取 FNOS 原生媒体库列表
func (p *PosterPrefetcher) fetchFnosLibraries(authHeaders http.Header) (map[string]string, error) {
	p.server.proxyMu.RLock()
	targetURL := p.server.targetURL
	p.server.proxyMu.RUnlock()

	req, err := http.NewRequestWithContext(context.Background(), "GET", targetURL.Scheme+"://"+targetURL.Host+"/v/api/v1/mediadb/list", nil)
	if err != nil {
		return nil, err
	}

	req.Header = authHeaders.Clone()
	// ✅ 关键修改：手动设置 Host 头，确保 FNOS 服务器能正确识别请求
	req.Host = targetURL.Host

	resp, err := p.server.retryClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status=%d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1*1024*1024))
	if err != nil {
		return nil, err
	}

	var fnosResp struct {
		Code int `json:"code"`
		Data []struct {
			GUID  string `json:"guid"`
			Title string `json:"title"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &fnosResp); err != nil {
		return nil, err
	}

	libs := make(map[string]string, len(fnosResp.Data))
	for _, item := range fnosResp.Data {
		if item.GUID != "" && item.Title != "" {
			libs[item.GUID] = item.Title
		}
	}
	return libs, nil
}

// fetchEmbyLibraries 拉取 Emby 兼容媒体库列表
func (p *PosterPrefetcher) fetchEmbyLibraries(authHeaders http.Header, userID string) (map[string]string, error) {
	p.server.proxyMu.RLock()
	targetURL := p.server.targetURL
	p.server.proxyMu.RUnlock()

	req, err := http.NewRequestWithContext(context.Background(), "GET", targetURL.Scheme+"://"+targetURL.Host+"/emby/Users/"+userID+"/Views", nil)
	if err != nil {
		return nil, err
	}

	req.Header = authHeaders.Clone()
	req.Host = targetURL.Host

	resp, err := p.server.retryClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status=%d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1*1024*1024))
	if err != nil {
		return nil, err
	}

	var embyResp struct {
		Items []struct {
			Id   string `json:"Id"`
			Name string `json:"Name"`
		} `json:"Items"`
	}
	if err := json.Unmarshal(body, &embyResp); err != nil {
		return nil, err
	}

	libs := make(map[string]string, len(embyResp.Items))
	for _, item := range embyResp.Items {
		if item.Id != "" && item.Name != "" {
			libs[item.Name] = item.Id
		}
	}
	return libs, nil
}

// ============================================================
// Emby 兼容 API 处理（爆米花/Vidhub/Infuse）
// ============================================================

// IsItemListRequest 判断是否是 Emby 兼容 API 的海报墙列表请求
func (p *PosterPrefetcher) IsItemListRequest(req *http.Request) (string, bool) {
	if req == nil || req.Method != "GET" {
		return "", false
	}
	pathLower := strings.ToLower(req.URL.Path)
	pathLower = strings.TrimPrefix(pathLower, "/emby")
	pathLower = strings.Trim(pathLower, "/")
	parts := strings.Split(pathLower, "/")
	if len(parts) < 3 {
		return "", false
	}
	itemsIdx := -1
	for i, seg := range parts {
		if seg == "items" {
			itemsIdx = i
			break
		}
	}
	if itemsIdx < 0 {
		return "", false
	}
	if itemsIdx < 2 || parts[itemsIdx-2] != "users" {
		return "", false
	}
	userID := parts[itemsIdx-1]
	if len(userID) < 16 || !util.IsGUIDLikeLoose(userID) {
		return "", false
	}
	remaining := parts[itemsIdx+1:]
	switch len(remaining) {
	case 0:
	case 1:
		if remaining[0] != "latest" {
			return "", false
		}
	default:
		return "", false
	}
	types := req.URL.Query().Get("IncludeItemTypes")
	if types == "" {
		if len(remaining) == 1 && remaining[0] == "latest" {
			return userID, true
		}
		return "", false
	}
	hasMovie := false
	hasSeries := false
	for _, t := range strings.Split(types, ",") {
		switch strings.ToLower(strings.TrimSpace(t)) {
		case "movie":
			hasMovie = true
		case "series":
			hasSeries = true
		}
	}
	if !hasMovie && !hasSeries {
		return "", false
	}
	return userID, true
}

// HandleListResponse 处理 Emby 兼容 API 响应
func (p *PosterPrefetcher) HandleListResponse(resp *http.Response, body []byte, userID string) {
	if resp != nil && resp.Request != nil {
		p.authStore.CaptureFromRequest(resp.Request)
	}
	if !config.Global.GetEnablePosterPrefetch() {
		return
	}
	if len(body) == 0 {
		return
	}

	data := body
	if resp != nil && strings.EqualFold(resp.Header.Get("Content-Encoding"), "gzip") {
		if decompressed := decompressGzipBody(body); decompressed != nil {
			data = decompressed
		}
	}

	rawItems, formatName := parseListResponse(data)
	if len(rawItems) == 0 {
		return
	}

	p.logger.Debug("🖼️ [海报墙预取] 解析到 %d 项 (格式=%s)", len(rawItems), formatName)

	if userID == "" {
		_, userID, _ = p.authStore.Get()
	}

	maxItems := config.Global.GetPosterPrefetchMaxItems()
	var items []PrefetchItem
	skippedSeries := 0
	for _, item := range rawItems {
		if item.Id == "" {
			continue
		}
		switch item.Type {
		case "Movie", "Video":
			items = append(items, PrefetchItem{
				ItemID: item.Id,
				UserID: userID,
				Name:   item.Name,
				Type:   "Movie",
			})
		case "Series":
			skippedSeries++
			continue
		default:
			continue
		}
		if maxItems > 0 && len(items) >= maxItems {
			break
		}
	}
	if len(items) == 0 {
		return
	}
	p.logger.Info("🖼️ [海报墙预取] 提取到 %d 部电影（列表 %d 项，跳过 %d 剧集），开始预取", len(items), len(rawItems), skippedSeries)

	authHeaders, _, _ := p.authStore.Get()
	go p.runBatchPrefetch(items, authHeaders)
}

func (p *PosterPrefetcher) runBatchPrefetch(items []PrefetchItem, authHeaders http.Header) {
	const chunkSize = 3
	totalStats := BatchStats{Total: len(items)}
	var failedItems []PrefetchItem

	for i := 0; i < len(items); i += chunkSize {
		end := i + chunkSize
		if end > len(items) {
			end = len(items)
		}
		chunk := items[i:end]
		stats := p.batch.PrefetchBatch(context.Background(), chunk, authHeaders, "poster", 600, "海报墙预取")
		totalStats.Success += stats.Success
		totalStats.Skipped += stats.Skipped
		totalStats.Failed += stats.Failed
		if stats.Failed > 0 {
			for _, item := range chunk {
				failedItems = append(failedItems, item)
			}
		}
		if i+chunkSize < len(items) {
			time.Sleep(1 * time.Second)
		}
	}

	p.logger.Info("🖼️ [海报墙预取] 首批完成: 成功=%d 跳过=%d 失败=%d", totalStats.Success, totalStats.Skipped, totalStats.Failed)

	if len(failedItems) > 0 {
		p.logger.Info("🖼️ [海报墙预取] 重试 %d 个失败项", len(failedItems))
		retrySuccess, retrySkipped, retryFailed := 0, 0, 0
		for i := 0; i < len(failedItems); i += 2 {
			end := i + 2
			if end > len(failedItems) {
				end = len(failedItems)
			}
			chunk := failedItems[i:end]
			stats := p.batch.PrefetchBatch(context.Background(), chunk, authHeaders, "poster_retry", 60, "海报墙预取-重试")
			retrySuccess += stats.Success
			retrySkipped += stats.Skipped
			retryFailed += stats.Failed
			if i+2 < len(failedItems) {
				time.Sleep(2 * time.Second)
			}
		}
		totalStats.Success += retrySuccess
		totalStats.Skipped += retrySkipped
		totalStats.Failed = retryFailed
	}

	p.logger.Info("🖼️ [海报墙预取] 最终完成: 成功=%d 跳过=%d 失败=%d", totalStats.Success, totalStats.Skipped, totalStats.Failed)
}

func parseListResponse(data []byte) ([]jsonItem, string) {
	var embyObj jsonListResponse
	if err := json.Unmarshal(data, &embyObj); err == nil && len(embyObj.Items) > 0 {
		return embyObj.Items, "emby-object"
	}
	var embyArr []jsonItem
	if err := json.Unmarshal(data, &embyArr); err == nil && len(embyArr) > 0 {
		return embyArr, "emby-array"
	}
	return nil, "unknown"
}

func decompressGzipBody(body []byte) []byte {
	gr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil
	}
	defer gr.Close()
	decompressed, err := io.ReadAll(io.LimitReader(gr, 10*1024*1024))
	if err != nil {
		return nil
	}
	return decompressed
}

// ============================================================
// ✅ 新增：强制洗白 Token 方法
// ============================================================

// forceUseLoginToken 强制使用主动登录的长效 Token
// 如果 AuthStore 中存有主动登录的 Token，则用它覆盖当前的请求头
func (p *PosterPrefetcher) forceUseLoginToken(headers http.Header) http.Header {
	// 1. 先通过 Get() 拿到 AuthStore 里的信息
	currentHeaders, _, expired := p.authStore.Get()
	if expired || currentHeaders == nil {
		return headers // 如果当前已过期，直接返回原 headers
	}

	// 2. 检查 AuthStore 里是否带有主动登录的 Token 特征
	// 因为主动登录的 Token 会被自动拼接到 X-Emby-Authorization 中
	if auth := currentHeaders.Get("X-Emby-Authorization"); auth != "" {
		// 如果包含 Token=，说明 AuthStore 里有长效 Token
		// 我们直接把这个长效的 Authorization 强制塞进当前的 headers 里
		headers.Set("X-Emby-Authorization", auth)
		if token := currentHeaders.Get("X-Emby-Token"); token != "" {
			headers.Set("X-Emby-Token", token)
		}
	}
	return headers
}
