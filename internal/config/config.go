package config

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/viper"
)

// Config 配置结构
type Config struct {
	ListenAddr     string        `mapstructure:"listen"`        // 反代监听地址
	TargetAddr     string        `mapstructure:"target"`        // 目标服务地址
	LogLevel       string        `mapstructure:"log_level"`     // 日志级别
	LogDir         string        `mapstructure:"log_dir"`       // 日志目录
	CacheTTL       time.Duration `mapstructure:"cache_ttl"`      // 缓存TTL（分钟）
	DashboardAddr  string        `mapstructure:"dashboard_addr"` // 管理面板地址
	DashboardUser  string        `mapstructure:"dashboard_user"` // 面板用户名
	DashboardPass  string        `mapstructure:"dashboard_pass"` // 面板密码
	MaxCacheItems  int           `mapstructure:"max_cache_items"` // 最大缓存条目数
	StrmPaths      []string      `mapstructure:"strm_paths"`    // STRM文件路径列表（容器内路径，用于扫描）
	StrmVolumes    []string      `mapstructure:"strm_volumes"`  // Docker Volume映射（完整格式：/host:/container:ro）
	StatsToken     string        `mapstructure:"stats_token"`   // /stats 端点访问令牌（空=公开，向后兼容）
	// STRM 解析模式
	//   - "auto"        智能模式（默认）：自动识别 302 直链/strm 工具 URL 跳过解析，其他走 HTTP 解析
	//   - "passthrough" 透传模式：所有 strm 直接把内容给播放器，不做任何解析/预热/预连接（适合全部 302 strm 场景）
	//   - "always"      始终解析模式：强制所有 strm 走 HTTP 解析
	StrmResolveMode string      `mapstructure:"strm_resolve_mode"`
	// 是否支持内网 strm 地址（默认 true）
	//   - true：反代解析内网地址 strm（需要容器能访问内网，建议用 host 网络模式）
	//   - false：跳过内网地址 strm，不解析（适合 bridge 网络模式且 strm 都是公网地址的场景）
	EnableLanStrm bool `mapstructure:"enable_lan_strm"`
	// 是否启用预加载（默认 true）
	//   - true：进入详情页/PlaybackInfo 时异步预加载解析 URL（推荐，首播快）
	//   - false：禁用所有预加载，仅按需解析（每次播放都要等解析，适合调试或资源受限场景）
	EnablePreload bool `mapstructure:"enable_preload"`
	// 是否启用智能签名 TTL 检测（默认 true）
	//   - true：解析 URL 后检测签名参数，按签名有效期动态设置缓存 TTL（解决播放中 403 问题）
	//   - false：使用配置的 cache_ttl 固定 TTL
	EnableSmartTTL bool `mapstructure:"enable_smart_ttl"`
	// 是否启用 CDN 预热（默认 true）
	//   - true：解析 URL 后异步预热 CDN 边缘节点，首播更快
	//   - false：禁用 CDN 预热，仅做 URL 解析（适合节省带宽或资源受限场景）
	EnableCDNWarmup bool `mapstructure:"enable_cdn_warmup"`
	mutex     sync.RWMutex
}

// Global 全局配置实例
var Global = &Config{
	ListenAddr:     ":28005",
	TargetAddr:     "http://127.0.0.1:8005",
	LogLevel:       "info",
	LogDir:         "./logs",
	CacheTTL:       30 * time.Minute, // 默认 30 分钟（配合智能签名 TTL，避免缓存过期晚于签名过期）
	DashboardAddr:  ":28006",
	DashboardUser:  "admin",
	DashboardPass:  "admin",
	MaxCacheItems:  10000,
	StrmPaths:      []string{"/vol00"},
	StrmVolumes:    []string{"/vol00:/vol00:ro"},
	StrmResolveMode: "auto", // 默认智能模式
	EnableLanStrm:  true,    // 默认支持内网 strm
	EnablePreload:  true,    // 默认启用预加载
	EnableSmartTTL: true,    // 默认启用智能签名 TTL
	EnableCDNWarmup: true,  // 默认启用 CDN 预热
}

// Load 加载配置
func Load(configPath string) error {
	viper.SetConfigType("yaml")

	if configPath != "" {
		viper.SetConfigFile(configPath)
	} else {
		viper.SetConfigName("config")
		viper.AddConfigPath(".")
		viper.AddConfigPath("/app/configs/")
		viper.AddConfigPath("/etc/fnysfd/")
	}

	viper.SetDefault("listen", ":28005")
	viper.SetDefault("target", "http://127.0.0.1:8005")
	viper.SetDefault("log_level", "info")
	viper.SetDefault("log_dir", "./logs")
	viper.SetDefault("cache_ttl", 30)  // 30 分钟（配合智能签名 TTL）
	viper.SetDefault("dashboard_addr", ":28006")
	viper.SetDefault("dashboard_user", "admin")
	viper.SetDefault("dashboard_pass", "admin")
	viper.SetDefault("max_cache_items", 10000)
	viper.SetDefault("strm_paths", []string{"/vol00"})
	viper.SetDefault("strm_volumes", []string{"/vol00:/vol00:ro"})
	viper.SetDefault("stats_token", "")
	viper.SetDefault("strm_resolve_mode", "auto")
	viper.SetDefault("enable_lan_strm", true)
	viper.SetDefault("enable_preload", true)
	viper.SetDefault("enable_smart_ttl", true)
	viper.SetDefault("enable_cdn_warmup", true)

	viper.SetEnvPrefix("FNTV")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	// configCreated 标记配置文件是否为本次首次创建
	// 仅在首次创建时回写，避免正常加载时覆盖用户手写的注释和格式
	configCreated := false
	if err := viper.ReadInConfig(); err != nil {
		if strings.Contains(err.Error(), "no such file") || strings.Contains(err.Error(), "Not Found") || strings.Contains(err.Error(), "cannot find") {
			log.Println("⚠️ 未找到配置文件，自动创建默认配置...")
			if createErr := createDefaultConfig(configPath); createErr != nil {
				return fmt.Errorf("创建默认配置文件失败: %w", createErr)
			}
			if readErr := viper.ReadInConfig(); readErr != nil {
				return fmt.Errorf("重新读取配置文件失败: %w", readErr)
			}
			configCreated = true
			log.Println("✅ 默认配置文件已创建: " + configPath)
		} else {
			return err
		}
	}

	if err := viper.Unmarshal(Global); err != nil {
		return err
	}

	Global.validateStrmPaths()

	Global.CacheTTL = time.Duration(viper.GetInt("cache_ttl")) * time.Minute

	log.Printf("✅ 配置加载完成: %s", viper.ConfigFileUsed())
	log.Printf("📦 反代监听: %s -> %s", Global.GetListenAddr(), Global.GetTargetAddr())
	log.Printf("🌐 管理面板: %s", Global.GetDashboardAddr())

	// 仅在首次创建配置文件时回写，避免正常加载时丢失用户注释/格式
	if configCreated {
		return Global.saveConfigToFile()
	}
	return nil
}

// validateStrmPaths 验证STRM路径是否可访问
func (c *Config) validateStrmPaths() {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if len(c.StrmPaths) == 0 {
		return
	}

	validCount := 0

	for _, path := range c.StrmPaths {
		if path == "" {
			continue
		}

		cleanPath := strings.ReplaceAll(path, "\\", "/")

		if _, err := os.Stat(cleanPath); err == nil {
			validCount++
		}
	}

	if validCount > 0 {
		log.Printf("✅ STRM路径配置: %d/%d 个可用", validCount, len(c.StrmPaths))
	}
}

// 配置监听停止通道（由 Watch 创建，由 Stop 关闭）
var (
	watchStopChan  chan struct{}
	watchStopOnce  sync.Once
	watchMutex     sync.Mutex // 保护 watchStopChan 赋值，防止 Watch 并发调用产生竞态
)

// Watch 监听配置变化
func Watch(onChange func()) {
	configFile := viper.ConfigFileUsed()
	if configFile == "" {
		log.Println("⚠️ 未找到配置文件，跳过监听")
		return
	}

	// 用 mutex 保护 watchStopChan 赋值，避免并发调用 Watch 时产生数据竞争
	watchMutex.Lock()
	watchStopChan = make(chan struct{})
	ch := watchStopChan
	watchMutex.Unlock()
	go pollConfigChanges(configFile, onChange, ch)
}

// Stop 停止配置监听（main.go 优雅关闭时调用）
func Stop() {
	watchStopOnce.Do(func() {
		watchMutex.Lock()
		defer watchMutex.Unlock()
		if watchStopChan != nil {
			close(watchStopChan)
			log.Println("🛑 配置监听已停止")
		}
	})
}

// pollConfigChanges 轮询检测配置文件变化
// 带防抖机制：文件变更后等待 1 秒无新变更再触发重载，
// 避免面板保存配置（原子写入：临时文件+rename）或连续编辑导致频繁 Reload 刷屏
func pollConfigChanges(configFile string, onChange func(), stopChan chan struct{}) {
	var lastModTime time.Time

	if info, err := os.Stat(configFile); err == nil {
		lastModTime = info.ModTime()
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	// 防抖定时器：检测到文件变更后等待 1 秒，期间若有新变更则重置计时
	// 面板保存配置时可能触发多次文件修改事件（原子写入 + viper.WriteConfig），
	// 防抖确保只触发一次 Reload
	var pendingChange bool
	debounce := time.NewTimer(time.Hour) // 初始状态：不会触发
	debounce.Stop()

	for {
		select {
		case <-ticker.C:
			info, err := os.Stat(configFile)
			if err != nil {
				continue
			}

			if info.ModTime().After(lastModTime) {
				lastModTime = info.ModTime()
				pendingChange = true
				// 重置防抖定时器（1秒后若无新变更则触发重载）
				debounce.Reset(1 * time.Second)
			}
		case <-debounce.C:
			if pendingChange {
				pendingChange = false
				handleConfigChange(configFile, onChange)
			}
		case <-stopChan:
			debounce.Stop()
			return
		}
	}
}

// handleConfigChange 处理配置变更
// ⚠️ 注意：viper 本身非线程安全，此函数对 viper 的读写操作依赖 Global.mutex 保护
// （Unmarshal 写入 Global 字段时持有写锁）。但 viper.SetConfigFile/ReadInConfig
// 与并发的 UpdateConfigWithStatus 中的 viper.WriteConfig 仍可能竞争。
// 当前实现假设配置文件变更频率低、且面板写入与文件监听不会同时发生，
// 如需更强一致性，应额外引入 viper 专用锁或使用单一 goroutine 串行化所有 viper 操作。
func handleConfigChange(configFile string, onChange func()) {
	log.Printf("📝 配置文件发生变化: %s", configFile)

	viper.SetConfigFile(configFile)
	if err := viper.ReadInConfig(); err != nil {
		log.Printf("❌ 读取配置文件失败: %v", err)
		return
	}

	Global.mutex.Lock()
	if err := viper.Unmarshal(Global); err != nil {
		Global.mutex.Unlock()
		log.Printf("❌ 解析配置失败: %v", err)
		return
	}
	Global.CacheTTL = time.Duration(viper.GetInt("cache_ttl")) * time.Minute
	Global.mutex.Unlock()

	log.Println("✅ 配置已热重载")

	if onChange != nil {
		onChange()
	}
}

// GetListenAddr 获取监听地址
func (c *Config) GetListenAddr() string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.ListenAddr
}

// GetTargetAddr 获取目标地址
func (c *Config) GetTargetAddr() string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.TargetAddr
}

// GetLogLevel 获取日志级别
func (c *Config) GetLogLevel() string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.LogLevel
}

// GetCacheTTL 获取缓存TTL
func (c *Config) GetCacheTTL() time.Duration {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.CacheTTL
}

// GetDashboardAddr 获取面板地址
func (c *Config) GetDashboardAddr() string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.DashboardAddr
}

// GetDashboardCredentials 获取面板认证信息
func (c *Config) GetDashboardCredentials() (string, string) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.DashboardUser, c.DashboardPass
}

// GetMaxCacheItems 获取最大缓存条目数
func (c *Config) GetMaxCacheItems() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.MaxCacheItems
}

// GetLogDir 获取日志目录
func (c *Config) GetLogDir() string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.LogDir
}

// GetStatsToken 获取/stats端点访问令牌（空字符串表示不启用认证）
func (c *Config) GetStatsToken() string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.StatsToken
}

// GetStrmResolveMode 获取STRM解析模式
// 返回值：auto（智能）/ passthrough（透传）/ always（始终解析）
// 空值或非法值统一返回 "auto"，保证向后兼容
func (c *Config) GetStrmResolveMode() string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	mode := strings.ToLower(strings.TrimSpace(c.StrmResolveMode))
	switch mode {
	case "auto", "passthrough", "always":
		return mode
	default:
		return "auto" // 默认智能模式（向后兼容）
	}
}

// GetEnableLanStrm 获取是否支持内网strm地址
// 默认 true（向后兼容：旧配置无此字段时 Unmarshal 零值 false，但默认值在 Global/viper.SetDefault 已设 true）
func (c *Config) GetEnableLanStrm() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.EnableLanStrm
}

// GetEnablePreload 获取是否启用预加载
// 默认 true
func (c *Config) GetEnablePreload() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.EnablePreload
}

// GetEnableSmartTTL 获取是否启用智能签名TTL检测
// 默认 true
func (c *Config) GetEnableSmartTTL() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.EnableSmartTTL
}

// GetEnableCDNWarmup 获取是否启用CDN预热
// 默认 true
func (c *Config) GetEnableCDNWarmup() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.EnableCDNWarmup
}

// SetLogLevel 设置日志级别
func (c *Config) SetLogLevel(level string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.LogLevel = level
}

// UpdateConfig 更新配置（用于面板修改）
func (c *Config) UpdateConfig(updates map[string]interface{}) error {
	_, err := c.UpdateConfigWithStatus(updates)
	return err
}

// UpdateConfigWithStatus 更新配置并返回是否需要重启
func (c *Config) UpdateConfigWithStatus(updates map[string]interface{}) (bool, error) {
	// 先做整体校验，校验失败则不修改任何字段（避免半修改状态）
	if err := validateConfigUpdates(updates); err != nil {
		return false, err
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	needsRestart := false

	for key, value := range updates {
		switch key {
		case "listen":
			if v, ok := value.(string); ok {
				if c.ListenAddr != v {
					c.ListenAddr = v
					// 端口已绑定，监听地址变更需重启才能生效
					needsRestart = true
					log.Println("🔄 检测到监听地址变更（需重启生效）...")
				}
			}
		case "target":
			if v, ok := value.(string); ok {
				if c.TargetAddr != v {
					c.TargetAddr = v
					needsRestart = true
				}
			}
		case "log_level":
			if v, ok := value.(string); ok {
				if c.LogLevel != v {
					c.LogLevel = v
					// 日志级别需要 logger.SetLevel 才能即时生效，
					// 标记 needsRestart 让上层 Reload/重启时应用新级别
					needsRestart = true
					log.Printf("🔄 检测到日志级别变更: %s（需重启生效）", v)
				}
			}
		case "log_dir":
			if v, ok := value.(string); ok {
				if c.LogDir != v {
					c.LogDir = v
					needsRestart = true
					log.Println("🔄 检测到日志目录变更（需重启生效）...")
				}
			}
		case "cache_ttl":
			var ttlMinutes int
			switch v := value.(type) {
			case int:
				ttlMinutes = v
			case float64:
				ttlMinutes = int(v)
			case string:
				if parsed, err := strconv.Atoi(v); err == nil {
					ttlMinutes = parsed
				}
			}
			if ttlMinutes > 0 {
				newTTL := time.Duration(ttlMinutes) * time.Minute
				if c.CacheTTL != newTTL {
					c.CacheTTL = newTTL
				}
			}
		case "dashboard_addr":
			if v, ok := value.(string); ok {
				if c.DashboardAddr != v {
					c.DashboardAddr = v
					// 面板端口已绑定，变更需重启才能生效
					needsRestart = true
					log.Println("🔄 检测到面板地址变更（需重启生效）...")
				}
			}
		case "dashboard_user":
			if v, ok := value.(string); ok {
				c.DashboardUser = v
			}
		case "dashboard_pass":
			if v, ok := value.(string); ok {
				// 前端 ToMap 返回 "****" 占位；若提交 "****" 或空则视为未修改
				if v == "" || v == "****" {
					continue
				}
				c.DashboardPass = v
			}
		case "max_cache_items":
			var maxItems int
			switch v := value.(type) {
			case int:
				maxItems = v
			case float64:
				maxItems = int(v)
			}
			if maxItems > 0 {
				c.MaxCacheItems = maxItems
			}
		case "strm_paths":
		// 类型断言失败时返回错误，不静默跳过（避免配置被意外清空）
		v, ok := value.([]interface{})
		if !ok {
			return false, fmt.Errorf("strm_paths 必须是字符串数组（当前类型: %T）", value)
		}
		paths := make([]string, 0, len(v))
		for _, path := range v {
			if s, ok := path.(string); ok && s != "" {
				paths = append(paths, s)
			}
		}
		oldPaths := c.StrmPaths
		c.StrmPaths = paths

		if !equalStringSlices(oldPaths, paths) {
			needsRestart = true
			log.Println("🔄 检测到STRM路径配置变更...")
		}
	case "strm_volumes":
		// 列表字段：完整替换；类型断言失败时返回错误
		v, ok := value.([]interface{})
		if !ok {
			return false, fmt.Errorf("strm_volumes 必须是字符串数组（当前类型: %T）", value)
		}
		volumes := make([]string, 0, len(v))
		for _, vol := range v {
			if s, ok := vol.(string); ok && s != "" {
				volumes = append(volumes, s)
			}
		}
		oldVolumes := c.StrmVolumes
		c.StrmVolumes = volumes
		if !equalStringSlices(oldVolumes, volumes) {
			needsRestart = true
			log.Println("🔄 检测到STRM Volume配置变更...")
		}
		case "stats_token":
			if v, ok := value.(string); ok {
				if v == "****" {
					continue
				}
				c.StatsToken = v
			}
		case "strm_resolve_mode":
			// STRM 解析模式（auto/passthrough/always）
			// 热重载生效，无需重启
			if v, ok := value.(string); ok {
				mode := strings.ToLower(strings.TrimSpace(v))
				switch mode {
				case "auto", "passthrough", "always":
					if c.StrmResolveMode != mode {
						c.StrmResolveMode = mode
						log.Printf("🔄 检测到STRM解析模式变更: %s（热重载生效）", mode)
					}
				default:
					return false, fmt.Errorf("strm_resolve_mode 必须是 auto/passthrough/always 之一（当前: %s）", v)
				}
			}
		case "enable_lan_strm":
			// 内网strm支持开关（热重载生效）
			if v, ok := value.(bool); ok {
				if c.EnableLanStrm != v {
					c.EnableLanStrm = v
					log.Printf("🔄 检测到内网strm支持变更: %v（热重载生效）", v)
				}
			}
		case "enable_preload":
			// 预加载开关（热重载生效）
			if v, ok := value.(bool); ok {
				if c.EnablePreload != v {
					c.EnablePreload = v
					log.Printf("🔄 检测到预加载开关变更: %v（热重载生效）", v)
				}
			}
		case "enable_smart_ttl":
			// 智能签名TTL开关（热重载生效）
			if v, ok := value.(bool); ok {
				if c.EnableSmartTTL != v {
					c.EnableSmartTTL = v
					log.Printf("🔄 检测到智能签名TTL开关变更: %v（热重载生效）", v)
				}
			}
		case "enable_cdn_warmup":
			// CDN预热开关（热重载生效）
			if v, ok := value.(bool); ok {
				if c.EnableCDNWarmup != v {
					c.EnableCDNWarmup = v
					log.Printf("🔄 检测到CDN预热开关变更: %v（热重载生效）", v)
				}
			}
		}
	}

	if err := c.saveConfigLocked(); err != nil {
		log.Printf("⚠️ 配置保存到文件失败: %v", err)
		return needsRestart, err
	}

	log.Println("✅ 配置已自动保存到文件")

	return needsRestart, nil
}

// validateConfigUpdates 校验配置更新项，校验失败返回错误
func validateConfigUpdates(updates map[string]interface{}) error {
	validLogLevels := map[string]bool{
		"trace": true, "debug": true, "info": true, "warn": true, "error": true,
	}

	for key, value := range updates {
		switch key {
		case "listen":
			if v, ok := value.(string); ok {
				if err := validateListenAddr(v); err != nil {
					return fmt.Errorf("listen 校验失败: %w", err)
				}
			}
		case "target":
			if v, ok := value.(string); ok {
				if !strings.HasPrefix(v, "http://") && !strings.HasPrefix(v, "https://") {
					return fmt.Errorf("target 必须以 http:// 或 https:// 开头")
				}
			}
		case "cache_ttl":
			var ttlMinutes int
			switch v := value.(type) {
			case int:
				ttlMinutes = v
			case float64:
				ttlMinutes = int(v)
			case string:
				if parsed, err := strconv.Atoi(v); err == nil {
					ttlMinutes = parsed
				}
			}
			if ttlMinutes < 1 || ttlMinutes > 100000 {
				return fmt.Errorf("cache_ttl 必须在 1-100000 分钟之间（当前: %d）", ttlMinutes)
			}
		case "max_cache_items":
			var maxItems int
			switch v := value.(type) {
			case int:
				maxItems = v
			case float64:
				maxItems = int(v)
			}
			if maxItems < 1 || maxItems > 1000000 {
				return fmt.Errorf("max_cache_items 必须在 1-1000000 之间（当前: %d）", maxItems)
			}
		case "log_level":
			if v, ok := value.(string); ok {
				if !validLogLevels[v] {
					return fmt.Errorf("log_level 必须是 trace/debug/info/warn/error 之一（当前: %s）", v)
				}
			}
		case "dashboard_user":
			if v, ok := value.(string); ok {
				if v == "" {
					return fmt.Errorf("dashboard_user 不能为空字符串")
				}
			}
		case "dashboard_pass":
			if v, ok := value.(string); ok {
				// "****" / 空视为未修改，跳过校验
				if v == "" || v == "****" {
					continue
				}
			}
		case "dashboard_addr":
		if v, ok := value.(string); ok {
			if err := validateListenAddr(v); err != nil {
				return fmt.Errorf("dashboard_addr 校验失败: %w", err)
			}
		}
	case "stats_token":
		// stats_token 基本校验：长度限制，避免过长的 token 拖慢每次请求的比对
		if v, ok := value.(string); ok {
			// "****" 视为未修改，跳过校验
			if v == "****" {
				continue
			}
			if len(v) > 256 {
				return fmt.Errorf("stats_token 长度不能超过 256 字符（当前: %d）", len(v))
			}
		}
	case "strm_resolve_mode":
		// STRM 解析模式校验
		if v, ok := value.(string); ok {
			mode := strings.ToLower(strings.TrimSpace(v))
			if mode != "auto" && mode != "passthrough" && mode != "always" {
				return fmt.Errorf("strm_resolve_mode 必须是 auto/passthrough/always 之一（当前: %s）", v)
			}
		}
	// bool 开关类配置无需额外校验（类型已由 UpdateConfig 的 type assertion 保证）
	case "enable_lan_strm", "enable_preload", "enable_smart_ttl", "enable_cdn_warmup":
		// bool 类型，无额外约束
	}
	}
	return nil
}

// validateListenAddr 校验监听地址
// 支持格式：
//   - ":port"        （仅端口，监听所有接口）
//   - "host:port"    （IPv4 或主机名）
//   - "[::1]:port"   （IPv6 地址，必须用方括号）
func validateListenAddr(addr string) error {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return fmt.Errorf("地址不能为空")
	}

	// 使用 net.SplitHostPort 解析地址，正确处理 IPv6 的 [host]:port 格式
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("地址格式无效（应为 host:port 或 [ipv6]:port）: %w", err)
	}
	_ = host // host 可为空（":port" 表示监听所有接口）

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("端口不是合法数字: %s", portStr)
	}
	if port < 1 || port > 65535 {
		return fmt.Errorf("端口范围必须在 1-65535 之间（当前: %d）", port)
	}
	return nil
}

// equalStringSlices 比较两个字符串切片是否相等
func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// saveConfigToFile 加锁后将当前内存配置原子写入 config.yaml 文件
func (c *Config) saveConfigToFile() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.saveConfigLocked()
}

// saveConfigLocked 原子写入配置文件（调用者必须已持有写锁）
// 通过 "写临时文件 + os.Rename" 实现原子替换，避免并发读取到半写状态
func (c *Config) saveConfigLocked() error {
	configFile := viper.ConfigFileUsed()
	if configFile == "" {
		return fmt.Errorf("未找到配置文件路径")
	}

	viper.Set("listen", c.ListenAddr)
	viper.Set("target", c.TargetAddr)
	viper.Set("log_level", c.LogLevel)
	viper.Set("log_dir", c.LogDir)
	viper.Set("cache_ttl", int(c.CacheTTL.Minutes()))
	viper.Set("dashboard_addr", c.DashboardAddr)
	viper.Set("dashboard_user", c.DashboardUser)
	viper.Set("dashboard_pass", c.DashboardPass)
	viper.Set("max_cache_items", c.MaxCacheItems)
	viper.Set("strm_paths", c.StrmPaths)
	viper.Set("strm_volumes", c.StrmVolumes)
	viper.Set("stats_token", c.StatsToken)
	viper.Set("strm_resolve_mode", c.StrmResolveMode) //
	viper.Set("enable_lan_strm", c.EnableLanStrm)      //
	viper.Set("enable_preload", c.EnablePreload)       //
	viper.Set("enable_smart_ttl", c.EnableSmartTTL)    //
	viper.Set("enable_cdn_warmup", c.EnableCDNWarmup)  //

	// 原子写入：先写临时文件（必须保留 .yaml 扩展名，否则 viper 无法识别格式），
	// 再通过 os.Rename 原子替换原文件
	dir := filepath.Dir(configFile)
	tmpFile, err := os.CreateTemp(dir, ".fnysfd-config-*.yaml")
	if err != nil {
		// 临时文件创建失败，回退到直接写入（非原子，但保证功能可用）
		if writeErr := viper.WriteConfig(); writeErr != nil {
			return fmt.Errorf("写入配置文件失败: %w", writeErr)
		}
		return nil
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()

	if err := viper.WriteConfigAs(tmpPath); err != nil {
		os.Remove(tmpPath)
		// 回退到直接写入
		if writeErr := viper.WriteConfig(); writeErr != nil {
			return fmt.Errorf("写入配置文件失败: %w", writeErr)
		}
		return nil
	}

	if err := os.Rename(tmpPath, configFile); err != nil {
		os.Remove(tmpPath)
		// Rename 失败（可能是跨设备），回退到直接写入
		if writeErr := viper.WriteConfig(); writeErr != nil {
			// 注意：这里包装 writeErr（直接写入的错误）而非 err（Rename 的错误），
			// 因为最终失败原因是 WriteConfig 失败
			return fmt.Errorf("替换配置文件失败（Rename: %v；WriteConfig: %w）", err, writeErr)
		}
		return nil
	}

	log.Printf("💾 配置已保存到: %s", configFile)
	return nil
}

// ToMap 导出配置为map（用于API返回）
func (c *Config) ToMap() map[string]interface{} {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	// dashboard_pass 不返回明文，前端用 "****" 占位识别；
	// 若需修改密码，前端提交新值（非 "****" / 空）即可
	dashPassMasked := ""
	if c.DashboardPass != "" {
		dashPassMasked = "****"
	}

	// stats_token 同样不返回明文
	statsTokenMasked := ""
	if c.StatsToken != "" {
		statsTokenMasked = "****"
	}

	return map[string]interface{}{
		"listen":           c.ListenAddr,
		"target":           c.TargetAddr,
		"log_level":        c.LogLevel,
		"log_dir":          c.LogDir,
		"cache_ttl":        int(c.CacheTTL.Minutes()),
		"dashboard_addr":   c.DashboardAddr,
		"dashboard_user":   c.DashboardUser,
		"dashboard_pass":   dashPassMasked,
		"max_cache_items":  c.MaxCacheItems,
		"strm_paths":       c.StrmPaths,
		"strm_volumes":     c.StrmVolumes,
		"stats_token":      statsTokenMasked,
		"strm_resolve_mode": c.StrmResolveMode, //
		"enable_lan_strm":  c.EnableLanStrm,    //
		"enable_preload":   c.EnablePreload,    //
		"enable_smart_ttl": c.EnableSmartTTL,   //
		"enable_cdn_warmup": c.EnableCDNWarmup, //
	}
}

// SetStrmPaths 设置STRM路径列表（完整替换）
func (c *Config) SetStrmPaths(paths []string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.StrmPaths = paths
	log.Printf("📁 STRM路径已更新: %v", paths)

	return c.saveConfigLocked()
}

// AddStrmVolume 添加STRM Volume映射（完整格式：主机路径 + 容器路径）
// 返回 (availableNow, error)：availableNow 为 true 表示 containerPath 在容器内已可访问，无需重建容器
func (c *Config) AddStrmVolume(hostPath, containerPath string) (bool, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	hostPath = strings.TrimSpace(hostPath)
	containerPath = strings.TrimSpace(containerPath)

	if hostPath == "" || containerPath == "" {
		return false, fmt.Errorf("参数错误：主机路径和容器路径都不能为空")
	}

	if !strings.HasPrefix(hostPath, "/") || !strings.HasPrefix(containerPath, "/") {
		return false, fmt.Errorf("参数错误：路径必须以 / 开头（主机路径：%s，容器路径：%s）", hostPath, containerPath)
	}

	if strings.Contains(hostPath, " ") || strings.Contains(containerPath, " ") {
		return false, fmt.Errorf("参数错误：路径不能包含空格")
	}

	volumeFormat := hostPath + ":" + containerPath + ":ro"

	for _, existing := range c.StrmVolumes {
		if existing == volumeFormat {
			return false, fmt.Errorf("该Volume映射已存在：%s", volumeFormat)
		}
	}

	c.StrmVolumes = append(c.StrmVolumes, volumeFormat)

	if !containsString(c.StrmPaths, containerPath) {
		c.StrmPaths = append(c.StrmPaths, containerPath)
	}

	log.Printf("➕ 添加STRM Volume: %s", volumeFormat)

	if err := c.saveConfigLocked(); err != nil {
		return false, err
	}

	// 检测容器内是否已可访问：如果 containerPath 已存在（说明宿主机目录已通过上层挂载点映射进来），
	// 则无需重建容器即可生效
	if _, err := os.Stat(containerPath); err == nil {
		log.Printf("✅ 路径在容器内已可访问，无需重建: %s", containerPath)
		return true, nil
	}
	return false, nil
}

// DeleteStrmVolume 删除STRM Volume映射
func (c *Config) DeleteStrmVolume(volumeFormat string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	volumeFormat = strings.TrimSpace(volumeFormat)
	newVolumes := make([]string, 0, len(c.StrmVolumes))
	found := false

	for _, v := range c.StrmVolumes {
		if v != volumeFormat {
			newVolumes = append(newVolumes, v)
		} else {
			found = true
			parts := strings.Split(v, ":")
			if len(parts) >= 2 {
				containerPath := parts[1]
				newPaths := make([]string, 0)
				for _, p := range c.StrmPaths {
					if p != containerPath {
						newPaths = append(newPaths, p)
					}
				}
				c.StrmPaths = newPaths
			}
		}
	}

	if !found {
		return fmt.Errorf("Volume映射不存在: %s", volumeFormat)
	}

	c.StrmVolumes = newVolumes
	log.Printf("➖ 删除STRM Volume: %s", volumeFormat)

	return c.saveConfigLocked()
}

// GetStrmVolumes 获取STRM Volume列表的副本
func (c *Config) GetStrmVolumes() []string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	volumes := make([]string, len(c.StrmVolumes))
	copy(volumes, c.StrmVolumes)
	return volumes
}

// GetStrmPaths 获取STRM路径列表的副本
func (c *Config) GetStrmPaths() []string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	paths := make([]string, len(c.StrmPaths))
	copy(paths, c.StrmPaths)
	return paths
}

// GenerateDockerVolumes 从StrmVolumes生成Docker Compose volumes配置
func (c *Config) GenerateDockerVolumes() []map[string]string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	volumes := make([]map[string]string, 0, len(c.StrmVolumes))

	for _, volumeFormat := range c.StrmVolumes {
		parts := strings.Split(volumeFormat, ":")
		if len(parts) >= 3 {
			volume := map[string]string{
				"host_path":      parts[0],
				"container_path": parts[1],
				"permission":     parts[2],
				"volume_format":  volumeFormat,
				"description":    "将NAS目录挂载到容器内（" + parts[2] + "）",
			}
			volumes = append(volumes, volume)
		}
	}

	return volumes
}

// containsString 检查字符串切片是否包含某个字符串
func containsString(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

// createDefaultConfig 创建默认配置文件
func createDefaultConfig(configPath string) error {
	if configPath == "" {
		// 优先从 CONFIG 环境变量读取，否则使用相对路径（跨平台兼容）
		configPath = os.Getenv("CONFIG")
		if configPath == "" {
			configPath = "./config.yaml"
		}
	}

	configDir := filepath.Dir(configPath)
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return fmt.Errorf("创建配置目录失败: %w", err)
	}

	// cache_ttl 默认 30 分钟，与 viper.SetDefault 保持一致
	defaultConfig := `cache_ttl: 30
dashboard_addr: :28006
dashboard_pass: admin
dashboard_user: admin
enable_lan_strm: true
enable_preload: true
enable_smart_ttl: true
enable_cdn_warmup: true
listen: :28005
log_dir: ./logs
log_level: info
max_cache_items: 10000
stats_token: ""
strm_paths:
    - /vol00
strm_resolve_mode: auto
strm_volumes:
    - /vol00:/vol00:ro
target: http://127.0.0.1:8005
`

	if err := os.WriteFile(configPath, []byte(defaultConfig), 0644); err != nil {
		return fmt.Errorf("写入配置文件失败: %w", err)
	}

	log.Printf("📝 已创建默认配置文件: %s", configPath)
	return nil
}

// UpdateComposeFile 自动更新docker-compose.yml文件（保存在原始项目目录）
// ⚠️ 注意：此操作会覆盖整个 docker-compose.yml 文件，用户自定义的配置会丢失。
// 建议仅在没有自定义配置时使用，或在更新后手动检查差异。
func (c *Config) UpdateComposeFile() error {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	configFile := viper.ConfigFileUsed()
	if configFile == "" {
		return fmt.Errorf("未找到配置文件路径")
	}

	configDir := filepath.Dir(configFile)
	projectDir := filepath.Dir(configDir)
	composeFile := filepath.Join(projectDir, "docker-compose.yml")

	// 列出所有 STRM Volume（不再硬编码排除 /vol00）
	volumeLines := ""
	for _, vol := range c.StrmVolumes {
		volumeLines += fmt.Sprintf("      - %s\n", vol)
	}

	// 模板更新说明：
	//   - 移除已废弃的 version: '3.8'（Compose v2+ 不再需要，且会触发告警）
	//   - 镜像标签保持与当前版本一致
	// ⚠️ 此操作会覆盖整个文件，请确保无自定义配置需保留
	composeContent := fmt.Sprintf(`# Docker Compose 配置（由管理面板自动生成，会覆盖原有内容）
# 如需保留自定义配置，请手动编辑而非使用此功能
services:
  fnysfd:
    image: fnysfd:3.3.11
    container_name: fnysfd
    restart: unless-stopped
    ports:
      - "28005:28005"
      - "28006:28006"
    volumes:
      - .:/data
%s    environment:
      - TZ=Asia/Shanghai
      - CONTAINER_NAME=fnysfd
    working_dir: /data
    networks:
      - fnysfd-network

networks:
  fnysfd-network:
    driver: bridge
`, volumeLines)

	if err := os.WriteFile(composeFile, []byte(composeContent), 0644); err != nil {
		return fmt.Errorf("写入compose文件失败: %w", err)
	}

	log.Printf("📝 Docker Compose文件已更新（原始位置）: %s", composeFile)
	return nil
}
