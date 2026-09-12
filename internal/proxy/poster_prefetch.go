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
// ⚠️ 海报墙预取触发已临时禁用（方案 A）
// 恢复时删除函数首行的 return 即可
// ✅ 终极修复：主动拉取 Emby 媒体库列表，不再依赖被动缓存
func (p *PosterPrefetcher) HandleFnosListRequest(reqBody []byte) {
	return // ⚠️ 临时禁用：直接返回，不触发任何库扫描

	if !config.Global.GetEnablePosterPrefetch() {
		return
	}

	// 120 秒全局去重，避免飞牛客户端频繁请求导致重复扫描
	key := "fnos-poster-trigger-global"
	if p.server.shouldSkipPrefetch(key, 120, "FNOS海报墙触发") {
		return
	}

	p.logger.Info("🖼️ [海报墙预取] 捕获 FNOS 海报墙请求，准备触发 Emby 库扫描")

	// --- 核心修改开始 ---
	// 1. 先尝试从缓存中获取
	p.mu.RLock()
	embyLibs := make(map[string]string, len(p.embyLibs))
	for k, v := range p.embyLibs {
		embyLibs[k] = v
	}
	p.mu.RUnlock()

	// 2. 如果缓存为空，主动刷新一次！
	if len(embyLibs) == 0 {
		p.logger.Warn("🖼️ [海报墙预取] Emby 媒体库缓存为空，正在主动刷新...")
		if p.refreshMapping() {
			// 刷新成功后，再次从缓存中获取
			p.mu.RLock()
			embyLibs = make(map[string]string, len(p.embyLibs))
			for k, v := range p.embyLibs {
				embyLibs[k] = v
			}
			p.mu.RUnlock()
			p.logger.Info("🖼️ [海报墙预取] 主动刷新成功，获取到 %d 个 Emby 库", len(embyLibs))
		} else {
			p.logger.Error("🖼️ [海报墙预取] 主动刷新失败，无法获取 Emby 媒体库列表")
			return
		}
	}
	// --- 核心修改结束 ---

	if len(embyLibs) == 0 {
		p.logger.Error("🖼️ [海报墙预取] 最终未能获取到任何 Emby 媒体库，放弃触发扫描")
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

// forceUseLoginToken 强制使用启动时登录获取的长效 Token
//
// 修复：防止被飞牛客户端的短效 Token 污染
func (p *PosterPrefetcher) forceUseLoginToken(currentHeaders http.Header) http.Header {
	// 1. 从 AuthStore 获取干净的、长效的 Token
	cleanHeaders, _, _ := p.authStore.Get()
	if cleanHeaders == nil {
		return currentHeaders // 防御性编程：如果拿不到，就用原来的
	}

	// 2. 克隆一份干净的 Header 作为基础
	newHeaders := cleanHeaders.Clone()

	// 3. 保留当前请求中可能存在的其他必要 Header（如 User-Agent）
	if ua := currentHeaders.Get("User-Agent"); ua != "" {
		newHeaders.Set("User-Agent", ua)
	}
	if accept := currentHeaders.Get("Accept"); accept != "" {
		newHeaders.Set("Accept", accept)
	}

	// 4. 关键：确保 X-Emby-Token 是长效的
	token := cleanHeaders.Get("X-Emby-Token")
	if token != "" {
		newHeaders.Set("X-Emby-Token", token)
		p.logger.Debug("🖼️ [海报墙预取] 强制使用长效 Token: %s", token[:8]+"...")
	}

	return newHeaders
}

// ============================================================
// Emby 兼容请求处理
// ============================================================

// HandleEmbyItemsRequest 处理 Emby 兼容的海报墙请求
//
// ⚠️ 海报墙预取触发已临时禁用（方案 A）
// 恢复时删除函数首行的 return 即可
// 策略：拦截 /Items 请求，解析 ParentId → 触发单库扫描
func (p *PosterPrefetcher) HandleEmbyItemsRequest(r *http.Request) {
	return // ⚠️ 临时禁用：直接返回，不触发任何库扫描

	if !config.Global.GetEnablePosterPrefetch() {
		return
	}

	parentID := r.URL.Query().Get("ParentId")
	if parentID == "" {
		return
	}

	// 120 秒去重
	key := "emby-poster-trigger:" + parentID
	if p.server.shouldSkipPrefetch(key, 120, "Emby海报墙触发") {
		return
	}

	p.logger.Info("🖼️ [海报墙预取] 捕获 Emby ParentId=%s，触发单库扫描", parentID)
	if p.server.libraryScanner != nil {
		go func() {
			_ = p.server.libraryScanner.ScanLibraryOnce(context.Background(), parentID)
		}()
	}
}

// ============================================================
// 工具函数
// ============================================================

// decompressGzip 解压 gzip 响应体
func decompressGzip(body []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

// isJSONResponse 判断是否为 JSON 响应
func isJSONResponse(contentType string) bool {
	return strings.Contains(contentType, "application/json")
}
