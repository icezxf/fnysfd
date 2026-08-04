package handler

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"fnysfd/internal/cache"
	"fnysfd/internal/logger"
	"fnysfd/internal/util"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
)

// PlaybackInfoResponse PlaybackInfo响应结构
type PlaybackInfoResponse struct {
	ItemID       string `json:"ItemId"`
	MediaSources []struct {
		ID       string `json:"Id"`
		Path     string `json:"Path"`
		Protocol string `json:"Protocol"`
		Name     string `json:"Name"` // 媒体源名称
	} `json:"MediaSources"`
}

// PlaybackHandler 处理PlaybackInfo
type PlaybackHandler struct {
	cache            *cache.Cache
	logger           *logger.Logger
	streamHandler    *StreamHandler // 引用StreamHandler用于预加载
	onPlaybackCached func(resp *http.Response, itemID string)
	processing       sync.Map // itemID -> struct{}{}，防止并发处理同一 ItemId 导致回调风暴
}

// NewPlaybackHandler 创建处理器
func NewPlaybackHandler(c *cache.Cache, l *logger.Logger) *PlaybackHandler {
	return &PlaybackHandler{
		cache:  c,
		logger: l,
	}
}

// SetStreamHandler 设置StreamHandler引用（必须在Start前调用）
func (h *PlaybackHandler) SetStreamHandler(sh *StreamHandler) {
	h.streamHandler = sh
	h.logger.Debug("🔗 [PlaybackInfo] StreamHandler已绑定")
}

// SetPlaybackCachedHandler 设置 PlaybackInfo 成功缓存 STRM 后的回调
func (h *PlaybackHandler) SetPlaybackCachedHandler(fn func(resp *http.Response, itemID string)) {
	h.onPlaybackCached = fn
}

// Handle 处理PlaybackInfo响应
func (h *PlaybackHandler) Handle(resp *http.Response, body []byte) ([]byte, error) {
	displayBody := body
	if resp.Header.Get("Content-Encoding") == "gzip" {
		if decompressed, err := decompressGzip(body); err == nil {
			displayBody = decompressed
		}
	}

	var playbackInfo PlaybackInfoResponse
	if err := json.Unmarshal(displayBody, &playbackInfo); err != nil {
		// JSON 解析失败时打 Warn，帮助诊断 PlaybackInfo 响应问题
		h.logger.Warn("⚠️ [PlaybackInfo] JSON解析失败: %v, Body长度=%d", err, len(displayBody))
		return body, nil
	}
	itemID := strings.TrimSpace(playbackInfo.ItemID)
	if itemID == "" {
		itemID = extractPlaybackItemIDFromRequest(resp)
		if itemID != "" {
			// 飞牛 API 已知行为：PlaybackInfo 响应体经常缺少 ItemId 字段
			// 降为 Debug 避免每次都刷屏（从请求路径补齐的逻辑已正常工作）
			h.logger.Debug("🧩 [PlaybackInfo] 响应体缺少ItemId，已从请求路径补齐: %s", itemID)
		}
	}

	// 去重检查：如果所有 STRM MediaSource 已缓存，说明已被其他路径处理过
	// （详情页预取 + 客户端 PlaybackInfo 请求会同时触发 Handle）
	// 跳过重复的缓存、日志和预加载，避免刷屏和无效 I/O
	hasStrm := false
	allStrmCached := true
	for _, source := range playbackInfo.MediaSources {
		if strings.HasSuffix(strings.ToLower(source.Path), ".strm") {
			hasStrm = true
			if _, found := h.cache.Get(source.ID); !found {
				allStrmCached = false
				break
			}
		}
	}
	if hasStrm && allStrmCached {
		h.logger.Debug("📋 [PlaybackInfo] MediaSource已缓存，跳过重复处理: ItemId=%s", itemID)
		// ⚠️ 关键修复：即使 MediaSource 已缓存，仍需触发下一集预取回调
		// 原因：用户播放已预取的集时（如第2集已被第1集的预取缓存），allStrmCached=true
		// 导致回调不触发，后续集（第3、4集）不会被预取，预取链断裂。
		// 回调内部会检查 X-Fnysfd-Next-Prefetch 头，预取触发的请求会被跳过（防止递归）。
		// 电影没有下一集，跳过下一集预取回调（避免无意义的 API 调用和日志噪音）
		firstStrmPath := ""
		for _, source := range playbackInfo.MediaSources {
			if strings.HasSuffix(strings.ToLower(source.Path), ".strm") {
				firstStrmPath = source.Path
				break
			}
		}
		if h.onPlaybackCached != nil && itemID != "" && !isMoviePath(firstStrmPath) {
			h.onPlaybackCached(resp, itemID)
		}
		return body, nil
	}

	// 并发去重：多个路径（详情页预取、客户端 PlaybackInfo、主动预请求、400重试等）
	// 可能同时调用 Handle 处理同一 itemID。使用 LoadOrStore 原子操作确保只有一个
	// goroutine 执行 STRM 缓存和回调，其余直接返回原始 body（不影响客户端响应）。
	// 解决日志中 "STRM缓存回调触发" 6次并发刷屏的问题。
	if hasStrm && !allStrmCached && itemID != "" {
		if _, loaded := h.processing.LoadOrStore(itemID, struct{}{}); loaded {
			h.logger.Debug("📋 [PlaybackInfo] 并发处理中，跳过: ItemId=%s", itemID)
			return body, nil
		}
		defer h.processing.Delete(itemID)
	}

	strmCount := 0

	// 收集 STRM 媒体源，待批量预解析
	var strmSources []cache.MediaSource

	for _, source := range playbackInfo.MediaSources {
		pathLower := strings.ToLower(source.Path)

		isStrm := strings.HasSuffix(pathLower, ".strm")

		if isStrm {
			// 提取剧集名称，优先用 MediaSource.Name，其次从 STRM 文件路径提取
			epName := strings.TrimSpace(source.Name)
			if epName == "" {
				epName = extractEpisodeName(source.Path)
			}

			// 只缓存MediaSource信息，预解析交给防抖定时器（避免海报墙浏览时大量无效解析）
			ms := cache.MediaSource{
				ID:       source.ID,
				ItemID:   itemID,
				Path:     source.Path,
				Protocol: source.Protocol,
				Name:     epName,
			}
			h.cache.Set(source.ID, ms)
			strmCount++
			strmSources = append(strmSources, ms)

			h.logger.Info("✅ [STRM缓存] 名称=%s | 路径=%s",
				epName, source.Path)
		}
	}

	if strmCount > 0 {
		// 日志加入剧集名称
		firstName := ""
		if len(strmSources) > 0 {
			firstName = strmSources[0].Name
		}
		if firstName != "" {
			h.logger.Info("📦 [缓存] %d 个STRM源: %s | %s", strmCount, itemID, firstName)
		} else {
			h.logger.Info("📦 [缓存] %d 个STRM源: %s", strmCount, itemID)
		}
	} else {
		h.logger.Debug("📋 [PlaybackInfo] 无STRM项: ItemId=%s, MediaSource数=%d",
			itemID, len(playbackInfo.MediaSources))
	}

	// 直接预解析（用户确认海报墙不会发 PlaybackInfo，只有进入详情页才会发，无需防抖）
	// 先同步预读 STRM 文件并缓存 URL，再入异步队列兜底
	//   - 同步预读：PlaybackInfo 响应处理时立即读取 strm 文件，直链/passthrough 场景 URL 立即就绪
	//   - 异步队列：兜底处理需要 HTTP 解析的场景（always 模式或 auto 非直链）
	//   - 效果：飞牛 probe 完成时直链已就绪，流请求直接命中缓存，省去 worker 调度延迟
	if len(strmSources) > 0 {
		for _, src := range strmSources {
			if h.streamHandler != nil {
				h.streamHandler.PreloadStrmSync(src)    // 同步预读（直链立即就绪）
				h.streamHandler.PreloadMediaSource(src) // 异步队列兜底（需要 HTTP 解析时生效）
			}
		}
		// 电影没有下一集，跳过下一集预取回调（避免无意义的 API 调用和日志噪音）
		if h.onPlaybackCached != nil && itemID != "" && !isMoviePath(strmSources[0].Path) {
			h.onPlaybackCached(resp, itemID)
		}
	}

	return body, nil
}

// ReadStrmFile 读取.strm文件
func ReadStrmFile(path string) (string, error) {
	cleanPath := strings.ReplaceAll(path, "\\", "/")
	cleanPath = strings.TrimSpace(cleanPath)

	// 仅保留前三个有意义的路径变体（去掉.strm扩展名的变体几乎不会成功）
	pathVariants := []string{
		cleanPath,
		strings.TrimPrefix(cleanPath, "/"),
		"/" + cleanPath,
	}

	var lastErr error
	for _, variant := range pathVariants {
		if variant == "" {
			continue
		}

		content, err := os.ReadFile(variant)
		if err == nil {
			// ✅ P0: STRM解析增加BOM/注释/多行处理
			// 去除UTF-8 BOM（EF BB BF）
			if len(content) >= 3 && content[0] == 0xEF && content[1] == 0xBB && content[2] == 0xBF {
				content = content[3:]
			}
			// 按行处理，取第一个非空非注释行作为URL
			url := ""
			for _, line := range strings.Split(string(content), "\n") {
				line = strings.TrimSpace(line)
				// 跳过空行和注释行（# 开头）
				if line == "" || strings.HasPrefix(line, "#") {
					continue
				}
				// 去除URL两端的引号（"或'）和尖括号（<>）
				line = strings.Trim(line, "\"'<>")
				line = strings.TrimSpace(line)
				if line != "" {
					url = line
					break
				}
			}
			if url != "" {
				return url, nil
			}
			lastErr = fmt.Errorf("STRM文件无有效URL: %s", variant)
		} else {
			lastErr = err
		}
	}

	if lastErr != nil {
		return "", lastErr
	}

	return "", fmt.Errorf("无法读取STRM文件: %s", path)
}

func decompressGzip(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func extractPlaybackItemIDFromRequest(resp *http.Response) string {
	if resp == nil || resp.Request == nil || resp.Request.URL == nil {
		return ""
	}
	return strings.TrimSpace(util.ExtractItemIDFromPath(resp.Request.URL.Path))
}
