package util

import "strings"

// IsGUIDLike 检查字符串是否为飞牛 ItemID 格式（32 位十六进制或 36 位带连字符的 UUID）
// 统一版本：同时校验长度和字符集，只允许十六进制字符（0-9, a-f, A-F）和连字符
func IsGUIDLike(s string) bool {
	// GUID 标准长度为 32（无连字符）或 36（带连字符）
	if len(s) != 32 && len(s) != 36 {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') ||
			(c >= 'A' && c <= 'F') || c == '-') {
			return false
		}
	}
	return true
}

// IsGUIDLikeLoose 宽松版 GUID 检查：只校验字符集，不校验长度
// 用于路径段初筛（长度可能不固定），调用方按需做长度二次校验
func IsGUIDLikeLoose(s string) bool {
	if len(s) == 0 {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') ||
			(c >= 'A' && c <= 'F') || c == '-') {
			return false
		}
	}
	return true
}

// ExtractItemIDFromPath 从 URL 路径提取 ItemID
// 路径格式：/emby/Items/{id}/PlaybackInfo 或 /emby/Users/{uid}/Items/{id}
// 找到 "items" 路径段后取下一段作为 ItemID
func ExtractItemIDFromPath(path string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if strings.ToLower(p) == "items" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}
