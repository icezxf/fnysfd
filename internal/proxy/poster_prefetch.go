package proxy

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fnysfd/internal/config"
	"fnysfd/internal/logger"
	"fnysfd/internal/util"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// PosterPrefetcher 海报墙预取器
//
// 支持两种 API 风格：
//  1. Emby 兼容 API（爆米花/Vidhub/Infuse）→ 精确预取
//  2. FNOS 原生 API（飞牛 Web/客户端）→ 触发单库扫描
//
// FNOS guid ≠ Emby ItemId（已实测验证），所以 FNOS 请求走"触发单库扫描"路径。
type PosterPrefetcher struct {
	server    *Server
	batch     *BatchPrefetcher
	logger    *logger.Logger
	authStore *AuthStore

	// ✅ 媒体库映射缓存（FNOS guid → Emby libId）
	mu       sync.RWMutex
	fnosLibs map[string]string // FNOS guid → FNOS title
	embyLibs map[string]string // Emby name → Emby libId
}

// NewPosterPrefetcher 创建海报墙预取器
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
// 媒体库列表缓存（供 FNOS guid → Emby libId 映射）
// ============================================================

// CacheEmbyLibraries 缓存 Emby 媒体库列表（拦截 /emby/Users/{uid}/Views 响应）
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
		if item.Id == "" || item.Name == "" {
			continue
		}
		p.embyLibs[item.Name] = item.Id
	}
	p.logger.Debug("🖼️ [海报墙预取] 缓存 Emby 媒体库: %d 个", len(p.embyLibs))
}

// CacheFnosLibraries 缓存 FNOS 媒体库列表（拦截 /v/api/v1/mediadb/list 响应）
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
		if item.GUID == "" || item.Title == "" {
			continue
		}
		p.fnosLibs[item.GUID] = item.Title
	}
	p.logger.Debug("🖼️ [海报墙预取] 缓存 FNOS 媒体库: %d 个", len(p.fnosLibs))
}

// mapFnosToEmby 通过媒体库名称匹配，把 FNOS guid 映射到 Emby libId
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
// FNOS 原生请求处理（供 server.go Director 调用）
// ============================================================

// HandleFnosListRequest 处理 FNOS 原生海报墙请求
//
// 从请求体拿 ancestor_guid（FNOS 媒体库 ID），映射到 Emby libId，
// 触发单库扫描。映射不到则 fallback 全库扫描。
// 30 秒去重避免飞牛客户端快速翻页触发多次。
func (p *PosterPrefetcher) HandleFnosListRequest(reqBody []byte) {
	if !config.Global.GetEnablePosterPrefetch() {
		return
	}

	var req struct {
		AncestorGUID string `json:"ancestor_guid"`
	}
	if err := json.Unmarshal(reqBody, &req); err != nil {
		return
	}
	if req.AncestorGUID == "" {
		return
	}

	embyLibID := p.mapFnosToEmby(req.AncestorGUID)

	if embyLibID == "" {
		// 映射不到 → fallback 全库扫描
		if p.server.shouldSkipPrefetch("fnos-poster-trigger", 30, "FNOS海报墙触发") {
			return
		}
		p.logger.Debug("🖼️ [海报墙预取] FNOS 媒体库 %s 未映射，fallback 全库扫描", req.AncestorGUID)
		if p.server.libraryScanner != nil {
			go func() {
				_ = p.server.libraryScanner.TriggerScan()
			}()
		}
		return
	}

	// 单库扫描，30 秒去重
	key := "fnos-poster-trigger:" + embyLibID
	if p.server.shouldSkipPrefetch(key, 30, "FNOS海报墙触发") {
		return
	}

	p.logger.Info("🖼️ [海报墙预取] FNOS 媒体库 %s → Emby %s，触发单库扫描",
		req.AncestorGUID, embyLibID)

	if p.server.libraryScanner != nil {
		go func() {
			_ = p.server.libraryScanner.ScanLibraryOnce(context.Background(), embyLibID)
		}()
	}
}

// ============================================================
// Emby 兼容 API 处理（爆米花/Vidhub/Infuse）
// ============================================================

// IsItemListRequest 判断是否是 Emby 兼容 API 的海报墙列表请求
//
// 注意：只认 Emby 风格（FNOS 原生请求由 server.go Director 直接拦截）
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

// HandleListResponse 处理 Emby 兼容 API 的海报墙响应
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

	p.logger.Info("🖼️ [海报墙预取] 提取到 %d 部电影（列表 %d 项，跳过 %d 剧集），开始预取",
		len(items), len(rawItems), skippedSeries)

	authHeaders, _, _ := p.authStore.Get()
	go p.runBatchPrefetch(items, authHeaders)
}

// runBatchPrefetch 分块预取 + 失败重试
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

	p.logger.Info("🖼️ [海报墙预取] 首批完成: 成功=%d 跳过=%d 失败=%d",
		totalStats.Success, totalStats.Skipped, totalStats.Failed)

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

	p.logger.Info("🖼️ [海报墙预取] 最终完成: 成功=%d 跳过=%d 失败=%d",
		totalStats.Success, totalStats.Skipped, totalStats.Failed)
}

// parseListResponse 解析 Emby 两种 JSON 格式
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
