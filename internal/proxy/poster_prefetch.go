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
// 拦截列表 API 响应，提取 ItemID 批量预取 PlaybackInfo。
// 用户浏览海报墙时，客户端会请求 /emby/Users/{uid}/Items?IncludeItemTypes=Movie，
// 此时反代拦截响应，提取所有 Movie 的 ItemID，异步批量预取 PlaybackInfo。
// 用户进入详情页时直链已就绪，点击播放直接命中缓存。
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
// 匹配规则：
//   - GET 方法
//   - 路径以 /emby/Users/{uid}/Items 结尾（最后一段是 Items）
//     或 /emby/Users/{uid}/Items/Latest
//   - query 含 IncludeItemTypes=Movie 或 Series（排除仅含 Episode 的请求）
//   - 排除路径以 /Items/{guid} 结尾的详情页请求
//
// 返回 (userID, ok)
func (p *PosterPrefetcher) IsItemListRequest(req *http.Request) (userID string, ok bool) {
	if req == nil {
		return "", false
	}
	if req.Method != "GET" {
		return "", false
	}

	// 统一路径：移除 /emby 前缀，转小写，去首尾斜杠
	path := req.URL.Path
	pathLower := strings.ToLower(path)
	pathLower = strings.TrimPrefix(pathLower, "/emby")
	pathLower = strings.Trim(pathLower, "/")

	parts := strings.Split(pathLower, "/")
	// 期望最少: users/{uid}/items
	if len(parts) < 3 {
		return "", false
	}

	// 查找 items 段的位置
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

	// items 前面必须是 users/{uid}
	if itemsIdx < 2 || parts[itemsIdx-2] != "users" {
		return "", false
	}
	userID = parts[itemsIdx-1]
	// UserID 应像 GUID（飞牛的 UserID 是 32 位十六进制）
	if len(userID) < 16 || !util.IsGUIDLikeLoose(userID) {
		return "", false
	}

	// items 后面的段：允许空（/Items 结尾）或 "latest"（/Items/Latest）
	// 其他情况（如 /Items/{guid} 详情页、/Items/{guid}/Images 子资源）排除
	remaining := parts[itemsIdx+1:]
	switch len(remaining) {
	case 0:
		// /Items 结尾，是列表请求
	case 1:
		// /Items/Latest，也是列表请求
		if remaining[0] != "latest" {
			return "", false
		}
	default:
		// /Items/Latest/xxx 或 /Items/{guid}/xxx，排除
		return "", false
	}

	// 检查 query 含 IncludeItemTypes=Movie 或 Series
	types := req.URL.Query().Get("IncludeItemTypes")
	if types == "" {
		// ✅ 放宽：如果路径是 /Items/Latest，允许不带 IncludeItemTypes
		// 飞牛 Web 客户端的 /Items/Latest 通常不带这个参数
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
	// 必须含 Movie 或 Series（排除仅含 Episode 的请求）
	if !hasMovie && !hasSeries {
		return "", false
	}

	return userID, true
}

// HandleListResponse 解析列表响应，提取 ItemID，调用 batch.PrefetchBatch
//
// 流程：
//  1. 从 resp.Request 捕获认证信息到 authStore（供全库扫描使用）
//  2. 检查功能开关
//  3. 解析 JSON 列表（✅ 兼容飞牛的数组格式和标准 Emby 对象格式）
//  4. 只取 Type="Movie" 的项目（Series 留给详情页预取链路）
//  5. 受 GetPosterPrefetchMaxItems 限制
//  6. ✅ 分块预取（每批 3 个）+ 批次间延迟 1s + 失败重试，避免飞牛 probe 拥堵
func (p *PosterPrefetcher) HandleListResponse(resp *http.Response, body []byte, userID string) {
	// 1. 捕获认证信息（无论功能是否开启，都需为全库扫描积累认证）
	if resp != nil && resp.Request != nil {
		p.authStore.CaptureFromRequest(resp.Request)
	}

	// 2. 检查功能开关
	if !config.Global.GetEnablePosterPrefetch() {
		return
	}

	if len(body) == 0 {
		p.logger.Debug("🖼️ [海报墙预取] 响应体为空，跳过")
		return
	}

	// 处理可能的 gzip 压缩
	data := body
	if resp != nil && strings.EqualFold(resp.Header.Get("Content-Encoding"), "gzip") {
		if decompressed := decompressGzipBody(body); decompressed != nil {
			data = decompressed
		}
	}

	// ✅ 3. 兼容两种格式解析：
	//    - 标准 Emby 对象: {"Items": [{"Id":"xxx","Type":"Movie"}, ...]}
	//    - 飞牛数组:       [{"Id":"xxx","Type":"Movie"}, ...]
	var rawItems []jsonItem

	var listResp jsonListResponse
	if err := json.Unmarshal(data, &listResp); err == nil && len(listResp.Items) > 0 {
		// 标准 Emby 对象格式
		rawItems = listResp.Items
	} else {
		// 尝试数组格式（飞牛某些端点直接返回数组）
		if err := json.Unmarshal(data, &rawItems); err != nil {
			p.logger.Warn("🖼️ [海报墙预取] JSON解析失败: %v, bodyLen=%d", err, len(data))
			return
		}
	}

	if len(rawItems) == 0 {
		p.logger.Debug("🖼️ [海报墙预取] 列表为空，跳过")
		return
	}

	// 4. 只取 Type="Movie" 的项目（Series 留给详情页预取链路）
	// 5. 受 GetPosterPrefetchMaxItems 限制
	maxItems := config.Global.GetPosterPrefetchMaxItems()
	var items []PrefetchItem
	for _, item := range rawItems {
		if item.Type != "Movie" {
			continue
		}
		if item.Id == "" {
			continue
		}
		items = append(items, PrefetchItem{
			ItemID: item.Id,
			UserID: userID,
			Name:   item.Name,
			Type:   item.Type,
		})
		if maxItems > 0 && len(items) >= maxItems {
			break
		}
	}

	if len(items) == 0 {
		p.logger.Debug("🖼️ [海报墙预取] 无 Movie 类型项目 (列表共 %d 项)", len(rawItems))
		return
	}

	p.logger.Info("🖼️ [海报墙预取] 提取到 %d 部电影（列表共 %d 项），开始批量预取",
		len(items), len(rawItems))

	// 6. ✅ 异步执行分块预取（每批 3 个）+ 批次间延迟 + 失败重试
	// 避免一次性把全部丢给飞牛，probe 互相竞争导致超时
	authHeaders, _, _ := p.authStore.Get()
	go func() {
		const chunkSize = 3
		totalStats := BatchStats{Total: len(items)}

		// 收集失败项
		var failedItems []PrefetchItem

		// 第一批：分块预取
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

			// 收集失败项（Failed > 0 说明有超时的）
			if stats.Failed > 0 {
				for _, item := range chunk {
					failedItems = append(failedItems, item)
				}
			}

			// 批次之间延迟 1 秒，给飞牛 probe 喘息时间
			if i+chunkSize < len(items) {
				time.Sleep(1 * time.Second)
			}
		}

		p.logger.Info("🖼️ [海报墙预取] 首批完成: 成功=%d 跳过=%d 失败=%d",
			totalStats.Success, totalStats.Skipped, totalStats.Failed)

		// ✅ 重试失败项（每批 2 个，间隔 2 秒）
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

			// 更新最终统计（重试成功后成功数增加，失败数用重试后的结果）
			totalStats.Success += retrySuccess
			totalStats.Skipped += retrySkipped
			totalStats.Failed = retryFailed
		}

		p.logger.Info("🖼️ [海报墙预取] 最终完成: 成功=%d 跳过=%d 失败=%d",
			totalStats.Success, totalStats.Skipped, totalStats.Failed)
	}()
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