package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// authStoreTTL 认证缓存有效期：超过此时间未更新视为过期
const authStoreTTL = 6 * time.Hour

// AuthStore 认证信息缓存
// 🔐 从用户请求中提取并缓存 Emby 认证信息（X-Emby-Token / Authorization），
// 供后台全库扫描任务复用，避免无认证请求被飞牛拒绝。
type AuthStore struct {
	mu        sync.RWMutex
	headers   http.Header // 缓存 X-Emby-Token、Authorization 等认证头
	userID    string      // 从 /emby/Users/{uid}/... 路径提取的用户ID
	updatedAt time.Time   // 最近一次更新时间
}

// NewAuthStore 创建认证缓存
func NewAuthStore() *AuthStore {
	return &AuthStore{
		headers: make(http.Header),
	}
}

// CaptureFromRequest 从用户请求中提取认证信息并缓存
// 📥 提取 X-Emby-Token、Authorization（Bearer token）认证头，
// 以及从路径 /emby/Users/{uid}/Items/... 提取 userID。
// 只在提取到有效认证信息（至少一个认证头）时才更新缓存，
// 避免无认证请求覆盖已有有效缓存。
//
// ✅ 关键修复：如果请求里没有 X-Emby-Token，但缓存里已有（来自主动登录），
// 则保留缓存里的 X-Emby-Token，避免被被动捕获的请求覆盖掉。
func (a *AuthStore) CaptureFromRequest(req *http.Request) {
	if req == nil {
		return
	}

	// 收集认证相关请求头
	captured := make(http.Header)
	token := req.Header.Get("X-Emby-Token")
	auth := req.Header.Get("Authorization")
	embyAuth := req.Header.Get("X-Emby-Authorization")

	if token != "" {
		captured.Set("X-Emby-Token", token)
	}
	if auth != "" {
		captured.Set("Authorization", auth)
	}
	if embyAuth != "" {
		captured.Set("X-Emby-Authorization", embyAuth)
	}

	// 只在提取到有效认证信息时才更新缓存
	if token == "" && auth == "" && embyAuth == "" {
		return
	}

	// 从路径提取 userID（形如 /emby/Users/{uid}/Items/...）
	userID := extractUserIDFromPath(req.URL.Path)

	a.mu.Lock()
	defer a.mu.Unlock()

	// ✅ 关键：保留已有的 X-Emby-Token（主动登录获取的），
	// 避免被动捕获的请求覆盖掉主动登录的 Token
	if a.headers.Get("X-Emby-Token") != "" && captured.Get("X-Emby-Token") == "" {
		captured.Set("X-Emby-Token", a.headers.Get("X-Emby-Token"))
	}

	a.headers = captured
	// 仅在新请求携带 userID 时更新，避免无 userID 的请求清空已有缓存
	if userID != "" {
		a.userID = userID
	}
	a.updatedAt = time.Now()
}

// Get 返回缓存的认证信息
// 🔓 返回 header 的副本（调用方可安全修改）和 userID。
// expired=true 表示缓存为空或超过 6 小时未更新。
func (a *AuthStore) Get() (headers http.Header, userID string, expired bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	// 缓存为空或从未更新过
	if len(a.headers) == 0 || a.updatedAt.IsZero() {
		return nil, "", true
	}

	expired = time.Since(a.updatedAt) > authStoreTTL
	return a.headers.Clone(), a.userID, expired
}

// IsReady 缓存是否就绪（非空且未过期）
// ✅ 后台全库扫描任务启动前调用此方法判断是否可复用认证信息。
func (a *AuthStore) IsReady() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if len(a.headers) == 0 || a.updatedAt.IsZero() {
		return false
	}
	return time.Since(a.updatedAt) <= authStoreTTL
}

// ============================================================
// ✅ 主动登录：通过 Emby 兼容的认证端点获取 AccessToken
// 这是解决"fnysfd 启动后 AuthStore 为空，需手动触发"的关键
// ============================================================

// LoginViaEmby 通过 Emby 兼容的认证端点主动登录，获取 AccessToken
// 使用飞牛影视的账号密码（注意：不是飞牛系统的账号）
func (a *AuthStore) LoginViaEmby(username, password, serverURL string) error {
	if username == "" || password == "" {
		return fmt.Errorf("用户名或密码为空")
	}

	// 构造登录请求体（Emby 标准格式）
	loginData := map[string]string{
		"Username": username,
		"Pw":       password,
	}
	jsonData, err := json.Marshal(loginData)
	if err != nil {
		return fmt.Errorf("构造请求体失败: %w", err)
	}

	// 构造 Emby 认证头
	embyAuth := `MediaBrowser Client="fnysfd", Device="fnysfd", DeviceId="fnysfd", Version="3.4.0"`

	// 尝试多个可能的端点（不同版本的飞牛可能路径不同）
	endpoints := []string{
		"/emby/Users/AuthenticateByName",
		"/Users/AuthenticateByName",
		"/emby/Users/authenticatebyname",
	}

	var lastErr error
	for _, endpoint := range endpoints {
		fullURL := serverURL + endpoint
		req, err := http.NewRequest("POST", fullURL, bytes.NewBuffer(jsonData))
		if err != nil {
			lastErr = err
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Emby-Authorization", embyAuth)
		req.Header.Set("Accept", "application/json")

		client := &http.Client{Timeout: 15 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("%s 请求失败: %w", endpoint, err)
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("%s 返回 %d: %s", endpoint, resp.StatusCode, string(body))
			continue
		}

		// 解析响应
		var loginResp struct {
			User struct {
				Id   string `json:"Id"`
				Name string `json:"Name"`
			} `json:"User"`
			AccessToken string `json:"AccessToken"`
		}
		if err := json.Unmarshal(body, &loginResp); err != nil {
			lastErr = fmt.Errorf("%s 解析失败: %w, body=%s", endpoint, err, string(body))
			continue
		}

		if loginResp.AccessToken == "" || loginResp.User.Id == "" {
			lastErr = fmt.Errorf("%s 响应缺少 AccessToken 或 User.Id, body=%s", endpoint, string(body))
			continue
		}

		// 成功！存入缓存
		a.mu.Lock()
		defer a.mu.Unlock()
		a.headers = make(http.Header)
		a.headers.Set("X-Emby-Token", loginResp.AccessToken)
		a.headers.Set("X-Emby-Authorization", `MediaBrowser Client="fnysfd", Device="fnysfd", DeviceId="fnysfd", Version="3.4.0", Token="`+loginResp.AccessToken+`"`)
		a.userID = loginResp.User.Id
		a.updatedAt = time.Now()

		return nil
	}

	return fmt.Errorf("所有端点均失败: %w", lastErr)
}

// extractUserIDFromPath 从 URL 路径提取 userID
// 路径格式：/emby/Users/{uid}/Items/...
func extractUserIDFromPath(path string) string {
	trimmed := strings.TrimPrefix(path, "/")
	if strings.HasPrefix(strings.ToLower(trimmed), "emby/") {
		trimmed = trimmed[5:]
	}

	parts := strings.Split(trimmed, "/")
	for i, p := range parts {
		if strings.EqualFold(p, "users") && i+1 < len(parts) {
			uid := parts[i+1]
			if uid != "" && !strings.EqualFold(uid, "me") {
				return uid
			}
		}
	}
	return ""
}
