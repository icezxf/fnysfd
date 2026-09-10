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
	mu         sync.RWMutex
	headers    http.Header
	userID     string
	loginToken string      // ✅ 主动登录的 Token，优先级最高，被动捕获不能覆盖
	updatedAt  time.Time
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
// ✅ 关键修复：如果主动登录过（loginToken 非空），则无论被动捕获到什么，
// 都用 loginToken 覆盖，避免被动捕获的旧 Token 覆盖主动登录的新 Token。
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

	// ✅ 关键：如果主动登录过，无论被动捕获到什么，都用 loginToken 覆盖
	if a.loginToken != "" {
		captured.Set("X-Emby-Token", a.loginToken)
		captured.Set("X-Emby-Authorization", `MediaBrowser Client="fnysfd", Device="fnysfd", DeviceId="fnysfd", Version="3.4.0", Token="`+a.loginToken+`"`)
	}

	a.headers = captured
	if userID != "" {
		a.userID = userID
	}
	a.updatedAt = time.Now()
}

// Get 返回缓存的认证信息
func (a *AuthStore) Get() (headers http.Header, userID string, expired bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if len(a.headers) == 0 || a.updatedAt.IsZero() {
		return nil, "", true
	}

	// ✅ 返回副本，并强制用 loginToken 覆盖
	headersCopy := a.headers.Clone()
	if a.loginToken != "" {
		headersCopy.Set("X-Emby-Token", a.loginToken)
		headersCopy.Set("X-Emby-Authorization", `MediaBrowser Client="fnysfd", Device="fnysfd", DeviceId="fnysfd", Version="3.4.0", Token="`+a.loginToken+`"`)
	}

	expired = time.Since(a.updatedAt) > authStoreTTL
	return headersCopy, a.userID, expired
}

// IsReady 缓存是否就绪
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
// ============================================================

// LoginViaEmby 通过 Emby 兼容的认证端点主动登录，获取 AccessToken
func (a *AuthStore) LoginViaEmby(username, password, serverURL string) error {
	if username == "" || password == "" {
		return fmt.Errorf("用户名或密码为空")
	}

	loginData := map[string]string{
		"Username": username,
		"Pw":       password,
	}
	jsonData, err := json.Marshal(loginData)
	if err != nil {
		return fmt.Errorf("构造请求体失败: %w", err)
	}

	embyAuth := `MediaBrowser Client="fnysfd", Device="fnysfd", DeviceId="fnysfd", Version="3.4.0"`

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
			lastErr = fmt.Errorf("%s 响应缺少 AccessToken 或 User.Id", endpoint)
			continue
		}

		// 成功！存入缓存
		a.mu.Lock()
		defer a.mu.Unlock()
		a.headers = make(http.Header)
		a.headers.Set("X-Emby-Token", loginResp.AccessToken)
		a.headers.Set("X-Emby-Authorization", `MediaBrowser Client="fnysfd", Device="fnysfd", DeviceId="fnysfd", Version="3.4.0", Token="`+loginResp.AccessToken+`"`)
		a.userID = loginResp.User.Id
		a.loginToken = loginResp.AccessToken  // ✅ 保存主动登录的 Token
		a.updatedAt = time.Now()

		return nil
	}

	return fmt.Errorf("所有端点均失败: %w", lastErr)
}

// extractUserIDFromPath 从 URL 路径提取 userID
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
