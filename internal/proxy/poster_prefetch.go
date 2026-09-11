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
	"time"
)

// PosterPrefetcher 海报墙预取器
//
// 支持两种 API 风格：
//  1. Emby 兼容 API（爆米花/Vidhub/Infuse 等第三方客户端）
//  2. FNOS 原生 API（飞牛 Web 客户端 / 飞牛影视客户端）
//
// ✅ 方案 A：海报墙只预取 Movie
//   电视剧交给全库扫描（Webhook 触发时后台跑），避免浏览海报墙时展开大量剧集阻塞
type PosterPrefetcher struct {
	server    *Server
	batch     *BatchPrefetcher
	logger    *logger.Logger
	authStore *AuthStore
}

// NewPosterPrefetcher 创建海报墙预取器
func NewPosterPrefetcher(s *Server, b *BatchPrefetcher, auth *AuthStore) *PosterPrefetcher {
	return &PosterPrefetcher{
		server:    s,
		batch:     b,
		logger:    s.logger,
		authStore: auth,
	}
}

// IsItemListRequest 判断是否是海报墙列表请求
//
// 匹配两种风格：
//  1. Emby: GET /emby/Users/{uid}/Items[/Latest]
//  2. FNOS 原生: POST /v/api/v1/item/list
func (p *PosterPrefetcher) IsItemListRequest(req *http.Request) (userID string, ok bool) {
	if req == nil {
		return "", false
	}

	pathLower := strings.ToLower(req.URL.Path)

	// ✅ FNOS 原生 API：POST /v/api/v1/item/list
	if req.Method == "POST" {
		trimmed := strings.TrimPrefix(pathLower, "/")
		if trimmed == "v/api/v1/item/list" {
			return "", true
		}
		return "", false
	}

	// 原有 Emby 兼容 API：GET
	if req.Method != "GET" {
		return "", false
	}

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
	userID = parts[itemsIdx-1]
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

// HandleListResponse 解析列表响应，提取 Movie 的 ItemID，批量预取
//
// 支持三种 JSON 格式：
//  1. Emby 对象: {"Items":[{"Id":"xxx","Type":"Movie"},...]}
//  2. Emby 数组: [{"Id":"xxx","Type":"Movie"},...]
//  3. FNOS 原生: {"code":0,"data":{"list":[{"guid":"xxx","type":"Movie"},...]}}
//
// ✅ 方案 A：只处理 Movie，Series/TV 跳过（交给全库扫描）
func (p *PosterPrefetcher) HandleListResponse(resp *http.Response, body []byte, userID string) {
	if resp != nil && resp.Request != nil {
		p.authStore.CaptureFromRequest(resp.Request)
	}

	if !config.Global.GetEnablePosterPrefetch() {
		return
	}

	if len(body) == 0 {
		p.logger.Debug("🖼️ [海报墙预取] 响应体为空，跳过")
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
		p.logger.Debug("🖼️ [海报墙预取] 列表为空或格式未知，跳过 (bodyLen=%d)", len(data))
		return
	}

	p.logger.Debug("🖼️ [海报墙预取] 解析到 %d 项 (格式=%s)", len(rawItems), formatName)

	if userID == "" {
		_, userID, _ = p.authStore.Get()
	}

	// ✅ 方案 A：只收集 Movie，跳过 Series/TV
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
			// ✅ 跳过剧集，交给全库扫描处理
			skippedSeries++
			continue
		default:
			continue
		}

		if maxItems > 0 && len(items) >= maxItems {
			p.logger.Debug("🖼️ [海报墙预取] 达到单次上限 %d，停止收集", maxItems)
			break
		}
	}

	if len(items) == 0 {
		if skippedSeries > 0 {
			p.logger.Debug("🖼️ [海报墙预取] 无 Movie 项目，跳过 %d 部剧集（交由全库扫描处理）", skippedSeries)
		} else {
			p.logger.Debug("🖼️ [海报墙预取] 无 Movie 类型项目 (列表共 %d 项)", len(rawItems))
		}
		return
	}

	p.logger.Info("🖼️ [海报墙预取] 提取到 %d 部电影（列表共 %d 项，跳过 %d 部剧集），开始批量预取",
		len(items), len(rawItems), skippedSeries)

	authHeaders, _, _ := p.authStore.Get()
	go func() {
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

			retrySuccess := 0
			retrySkipped := 0
			retryFailed := 0

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
	}()
}

// parseListResponse 尝试解析三种 JSON 格式，返回统一的 []jsonItem
func parseListResponse(data []byte) ([]jsonItem, string) {
	// 格式 1：Emby 对象 {"Items": [...]}
	var embyObj jsonListResponse
	if err := json.Unmarshal(data, &embyObj); err == nil && len(embyObj.Items) > 0 {
		return embyObj.Items, "emby-object"
	}

	// 格式 2：Emby 数组 [{"Id":...,"Name":...,"Type":...}]
	var embyArr []jsonItem
	if err := json.Unmarshal(data, &embyArr); err == nil && len(embyArr) > 0 {
		return embyArr, "emby-array"
	}

	// 格式 3：FNOS 原生 {"code":0,"data":{"list":[...]}}
	var fnosResp struct {
		Code int `json:"code"`
		Data struct {
			List []struct {
				GUID  string `json:"guid"`
				Title string `json:"title"`
				Type  string `json:"type"`
			} `json:"list"`
		} `json:"data"`
	}
	if err := json.Unmarshal(data, &fnosResp); err == nil && len(fnosResp.Data.List) > 0 {
		items := make([]jsonItem, 0, len(fnosResp.Data.List))
		for _, it := range fnosResp.Data.List {
			if it.GUID == "" {
				continue
			}
			normalizedType := it.Type
			switch it.Type {
			case "TV":
				normalizedType = "Series"
			case "Video":
				normalizedType = "Movie"
			}
			items = append(items, jsonItem{
				Id:   it.GUID,
				Name: it.Title,
				Type: normalizedType,
			})
		}
		return items, "fnos-native"
	}

	return nil, "unknown"
}

// decompressGzipBody 解压 gzip 压缩的响应体
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