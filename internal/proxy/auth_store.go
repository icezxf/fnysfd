package proxy

import (
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
func (a *AuthStore) CaptureFromRequest(req *http.Request) {
	if req == nil {
		return
	}

	// 收集认证相关请求头
	captured := make(http.Header)
	token := req.Header.Get("X-Emby-Token")
	auth := req.Header.Get("Authorization")

	if token != "" {
		captured.Set("X-Emby-Token", token)
	}
	if auth != "" {
		// Bearer token 或其他 Authorization 凭据
		captured.Set("Authorization", auth)
	}

	// 只在提取到有效认证信息时才更新缓存
	if token == "" && auth == "" {
		return
	}

	// 从路径提取 userID（形如 /emby/Users/{uid}/Items/...）
	userID := extractUserIDFromPath(req.URL.Path)

	a.mu.Lock()
	defer a.mu.Unlock()
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

// extractUserIDFromPath 从 URL 路径提取 userID
// 路径格式：/emby/Users/{uid}/Items/...
// 保留 userID 原始大小写（Emby 的 userId 通常是 GUID，但稳妥起见不做大小写转换）
func extractUserIDFromPath(path string) string {
	// 移除前导斜杠
	trimmed := strings.TrimPrefix(path, "/")
	// 移除 emby/ 前缀（不区分大小写）
	if strings.HasPrefix(strings.ToLower(trimmed), "emby/") {
		trimmed = trimmed[5:]
	}

	parts := strings.Split(trimmed, "/")
	for i, p := range parts {
		if strings.EqualFold(p, "users") && i+1 < len(parts) {
			uid := parts[i+1]
			// 排除空值和 "me"（Emby 的当前用户占位符）
			if uid != "" && !strings.EqualFold(uid, "me") {
				return uid
			}
		}
	}
	return ""
}
