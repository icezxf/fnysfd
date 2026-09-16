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
	ListenAddr     string        `mapstructure:"listen"`
	TargetAddr     string        `mapstructure:"target"`
	LogLevel       string        `mapstructure:"log_level"`
	LogDir         string        `mapstructure:"log_dir"`
	CacheTTL       time.Duration `mapstructure:"cache_ttl"`
	DashboardAddr  string        `mapstructure:"dashboard_addr"`
	DashboardUser  string        `mapstructure:"dashboard_user"`
	DashboardPass  string        `mapstructure:"dashboard_pass"`
	MaxCacheItems  int           `mapstructure:"max_cache_items"`
	StrmPaths      []string      `mapstructure:"strm_paths"`
	StrmVolumes    []string      `mapstructure:"strm_volumes"`
	StatsToken     string        `mapstructure:"stats_token"`
	StrmResolveMode string       `mapstructure:"strm_resolve_mode"`
	EnableLanStrm   bool         `mapstructure:"enable_lan_strm"`
	EnablePreload   bool         `mapstructure:"enable_preload"`
	EnableSmartTTL  bool         `mapstructure:"enable_smart_ttl"`
	EnableCDNWarmup bool         `mapstructure:"enable_cdn_warmup"`

	EnableLibraryScan             bool   `mapstructure:"enable_library_scan"`
	LibraryScanCron               string `mapstructure:"library_scan_cron"`
	LibraryScanOnStart            bool   `mapstructure:"library_scan_on_start"`
	LibraryScanConcurrency        int    `mapstructure:"library_scan_concurrency"`
	LibraryScanIntervalMs         int    `mapstructure:"library_scan_interval_ms"`
	LibraryScanIncrementalMinutes int    `mapstructure:"library_scan_incremental_minutes"`

	WebhookNotifyToken        string `mapstructure:"webhook_notify_token"`
	WebhookNotifyDelaySeconds int    `mapstructure:"webhook_notify_delay_seconds"`

	EnableDoubanRating bool `mapstructure:"enable_douban_rating"`

	FnosUsername string `mapstructure:"fnos_username"`
	FnosPassword string `mapstructure:"fnos_password"`

	// ✅ 观看记录数据库路径（空=禁用「观看记录」tab）
	PlayHistoryDBPath string `mapstructure:"play_history_db_path"`

	mutex sync.RWMutex
}

// Global 全局配置实例
var Global = &Config{
	ListenAddr:      ":28005",
	TargetAddr:      "http://127.0.0.1:8005",
	LogLevel:        "info",
	LogDir:          "./logs",
	CacheTTL:        30 * time.Minute,
	DashboardAddr:   ":28006",
	DashboardUser:   "admin",
	DashboardPass:   "admin",
	MaxCacheItems:   10000,
	StrmPaths:       []string{"/vol00"},
	StrmVolumes:     []string{"/vol00:/vol00:ro"},
	StrmResolveMode: "auto",
	EnableLanStrm:   true,
	EnablePreload:   true,
	EnableSmartTTL:  true,
	EnableCDNWarmup: true,

	EnableLibraryScan:             false,
	LibraryScanCron:               "03:00",
	LibraryScanOnStart:            false,
	LibraryScanConcurrency:        2,
	LibraryScanIntervalMs:         500,
	LibraryScanIncrementalMinutes: 5,

	WebhookNotifyToken:        "",
	WebhookNotifyDelaySeconds: 15,

	EnableDoubanRating: true,

	FnosUsername: "",
	FnosPassword: "",

	PlayHistoryDBPath: "/db/trimmedia.db",
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
	viper.SetDefault("cache_ttl", 30)
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
	viper.SetDefault("enable_library_scan", false)
	viper.SetDefault("library_scan_cron", "03:00")
	viper.SetDefault("library_scan_on_start", false)
	viper.SetDefault("library_scan_concurrency", 2)
	viper.SetDefault("library_scan_interval_ms", 500)
	viper.SetDefault("library_scan_incremental_minutes", 5)
	viper.SetDefault("webhook_notify_token", "")
	viper.SetDefault("webhook_notify_delay_seconds", 15)
	viper.SetDefault("enable_douban_rating", true)
	viper.SetDefault("fnos_username", "")
	viper.SetDefault("fnos_password", "")
	// ✅ 观看记录
	viper.SetDefault("play_history_db_path", "/db/trimmedia.db")

	viper.SetEnvPrefix("FNTV")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

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

	if configCreated {
		return Global.saveConfigToFile()
	}
	return nil
}

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

var (
	watchStopChan chan struct{}
	watchStopOnce sync.Once
	watchMutex    sync.Mutex
)

func Watch(onChange func()) {
	configFile := viper.ConfigFileUsed()
	if configFile == "" {
		log.Println("⚠️ 未找到配置文件，跳过监听")
		return
	}
	watchMutex.Lock()
	watchStopChan = make(chan struct{})
	ch := watchStopChan
	watchMutex.Unlock()
	go pollConfigChanges(configFile, onChange, ch)
}

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

func pollConfigChanges(configFile string, onChange func(), stopChan chan struct{}) {
	var lastModTime time.Time
	if info, err := os.Stat(configFile); err == nil {
		lastModTime = info.ModTime()
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	var pendingChange bool
	debounce := time.NewTimer(time.Hour)
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

// ========== Getter ==========

func (c *Config) GetListenAddr() string { c.mutex.RLock(); defer c.mutex.RUnlock(); return c.ListenAddr }
func (c *Config) GetTargetAddr() string { c.mutex.RLock(); defer c.mutex.RUnlock(); return c.TargetAddr }
func (c *Config) GetLogLevel() string   { c.mutex.RLock(); defer c.mutex.RUnlock(); return c.LogLevel }
func (c *Config) GetCacheTTL() time.Duration { c.mutex.RLock(); defer c.mutex.RUnlock(); return c.CacheTTL }
func (c *Config) GetDashboardAddr() string { c.mutex.RLock(); defer c.mutex.RUnlock(); return c.DashboardAddr }
func (c *Config) GetDashboardCredentials() (string, string) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.DashboardUser, c.DashboardPass
}
func (c *Config) GetMaxCacheItems() int { c.mutex.RLock(); defer c.mutex.RUnlock(); return c.MaxCacheItems }
func (c *Config) GetLogDir() string     { c.mutex.RLock(); defer c.mutex.RUnlock(); return c.LogDir }
func (c *Config) GetStatsToken() string { c.mutex.RLock(); defer c.mutex.RUnlock(); return c.StatsToken }

func (c *Config) GetStrmResolveMode() string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	mode := strings.ToLower(strings.TrimSpace(c.StrmResolveMode))
	switch mode {
	case "auto", "passthrough", "always":
		return mode
	default:
		return "auto"
	}
}

func (c *Config) GetEnableLanStrm() bool   { c.mutex.RLock(); defer c.mutex.RUnlock(); return c.EnableLanStrm }
func (c *Config) GetEnablePreload() bool   { c.mutex.RLock(); defer c.mutex.RUnlock(); return c.EnablePreload }
func (c *Config) GetEnableSmartTTL() bool  { c.mutex.RLock(); defer c.mutex.RUnlock(); return c.EnableSmartTTL }
func (c *Config) GetEnableCDNWarmup() bool { c.mutex.RLock(); defer c.mutex.RUnlock(); return c.EnableCDNWarmup }
func (c *Config) GetEnableLibraryScan() bool { c.mutex.RLock(); defer c.mutex.RUnlock(); return c.EnableLibraryScan }
func (c *Config) GetLibraryScanCron() string { c.mutex.RLock(); defer c.mutex.RUnlock(); return c.LibraryScanCron }
func (c *Config) GetLibraryScanOnStart() bool { c.mutex.RLock(); defer c.mutex.RUnlock(); return c.LibraryScanOnStart }

func (c *Config) GetLibraryScanConcurrency() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if c.LibraryScanConcurrency < 1 {
		return 2
	}
	return c.LibraryScanConcurrency
}

func (c *Config) GetLibraryScanIntervalMs() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if c.LibraryScanIntervalMs < 0 {
		return 500
	}
	return c.LibraryScanIntervalMs
}

func (c *Config) GetLibraryScanIncrementalMinutes() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if c.LibraryScanIncrementalMinutes < 1 {
		return 5
	}
	if c.LibraryScanIncrementalMinutes > 1440 {
		return 1440
	}
	return c.LibraryScanIncrementalMinutes
}

func (c *Config) GetWebhookNotifyToken() string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.WebhookNotifyToken
}

func (c *Config) GetWebhookNotifyDelaySeconds() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if c.WebhookNotifyDelaySeconds < 0 {
		return 15
	}
	if c.WebhookNotifyDelaySeconds > 300 {
		return 300
	}
	return c.WebhookNotifyDelaySeconds
}

func (c *Config) GetEnableDoubanRating() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.EnableDoubanRating
}

func (c *Config) GetFnosUsername() string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.FnosUsername
}

func (c *Config) GetFnosPassword() string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.FnosPassword
}

// ✅ 观看记录数据库路径
func (c *Config) GetPlayHistoryDBPath() string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.PlayHistoryDBPath
}

func (c *Config) SetLogLevel(level string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.LogLevel = level
}

func (c *Config) UpdateConfig(updates map[string]interface{}) error {
	_, err := c.UpdateConfigWithStatus(updates)
	return err
}

func (c *Config) UpdateConfigWithStatus(updates map[string]interface{}) (bool, error) {
	if err := validateConfigUpdates(updates); err != nil {
		return false, err
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	needsRestart := false

	for key, value := range updates {
		switch key {
		case "listen":
			if v, ok := value.(string); ok && c.ListenAddr != v {
				c.ListenAddr = v
				needsRestart = true
				log.Println("🔄 检测到监听地址变更（需重启生效）...")
			}
		case "target":
			if v, ok := value.(string); ok && c.TargetAddr != v {
				c.TargetAddr = v
				needsRestart = true
			}
		case "log_level":
			if v, ok := value.(string); ok && c.LogLevel != v {
				c.LogLevel = v
				needsRestart = true
				log.Printf("🔄 检测到日志级别变更: %s（需重启生效）", v)
			}
		case "log_dir":
			if v, ok := value.(string); ok && c.LogDir != v {
				c.LogDir = v
				needsRestart = true
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
				c.CacheTTL = time.Duration(ttlMinutes) * time.Minute
			}
		case "dashboard_addr":
			if v, ok := value.(string); ok && c.DashboardAddr != v {
				c.DashboardAddr = v
				needsRestart = true
			}
		case "dashboard_user":
			if v, ok := value.(string); ok {
				c.DashboardUser = v
			}
		case "dashboard_pass":
			if v, ok := value.(string); ok {
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
			if !equalStringSlices(c.StrmPaths, paths) {
				c.StrmPaths = paths
				needsRestart = true
			}
		case "strm_volumes":
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
			if !equalStringSlices(c.StrmVolumes, volumes) {
				c.StrmVolumes = volumes
				needsRestart = true
			}
		case "stats_token":
			if v, ok := value.(string); ok {
				if v == "****" {
					continue
				}
				c.StatsToken = v
			}
		case "strm_resolve_mode":
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
			if v, ok := value.(bool); ok {
				c.EnableLanStrm = v
			}
		case "enable_preload":
			if v, ok := value.(bool); ok {
				c.EnablePreload = v
			}
		case "enable_smart_ttl":
			if v, ok := value.(bool); ok {
				c.EnableSmartTTL = v
			}
		case "enable_cdn_warmup":
			if v, ok := value.(bool); ok {
				c.EnableCDNWarmup = v
			}
		case "enable_library_scan":
			if v, ok := value.(bool); ok {
				c.EnableLibraryScan = v
			}
		case "library_scan_cron":
			if v, ok := value.(string); ok {
				c.LibraryScanCron = v
			}
		case "library_scan_on_start":
			if v, ok := value.(bool); ok {
				c.LibraryScanOnStart = v
			}
		case "library_scan_concurrency":
			if v := toInt(value); v >= 1 && v <= 20 {
				c.LibraryScanConcurrency = v
			}
		case "library_scan_interval_ms":
			if v := toInt(value); v >= 0 && v <= 60000 {
				c.LibraryScanIntervalMs = v
			}
		case "library_scan_incremental_minutes":
			if v := toInt(value); v >= 1 && v <= 1440 {
				if c.LibraryScanIncrementalMinutes != v {
					c.LibraryScanIncrementalMinutes = v
					needsRestart = true
					log.Printf("🔄 检测到增量扫描间隔变更: %d 分钟（重启后生效）", v)
				}
			}
		case "webhook_notify_token":
			if v, ok := value.(string); ok {
				if v == "****" {
					continue
				}
				c.WebhookNotifyToken = v
			}
		case "webhook_notify_delay_seconds":
			if v := toInt(value); v >= 0 && v <= 300 {
				c.WebhookNotifyDelaySeconds = v
			}
		case "enable_douban_rating":
			if v, ok := value.(bool); ok {
				if c.EnableDoubanRating != v {
					c.EnableDoubanRating = v
					log.Printf("🔄 检测到豆瓣评分开关变更: %v（热重载生效）", v)
				}
			}
		case "fnos_username":
			if v, ok := value.(string); ok {
				if c.FnosUsername != v {
					c.FnosUsername = v
					needsRestart = true
					log.Println("🔄 检测到飞牛账号变更（需重启生效）")
				}
			}
		case "fnos_password":
			if v, ok := value.(string); ok {
				if v == "****" || v == "" {
					continue
				}
				if c.FnosPassword != v {
					c.FnosPassword = v
					needsRestart = true
					log.Println("🔄 检测到飞牛密码变更（需重启生效）")
				}
			}
		// ✅ 观看记录数据库路径
		case "play_history_db_path":
			if v, ok := value.(string); ok {
				if c.PlayHistoryDBPath != v {
					c.PlayHistoryDBPath = v
					needsRestart = true
					log.Println("🔄 检测到观看记录数据库路径变更（需重启生效）")
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
			if v, ok := value.(string); ok && v == "" {
				return fmt.Errorf("dashboard_user 不能为空字符串")
			}
		case "dashboard_pass":
			if v, ok := value.(string); ok {
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
			if v, ok := value.(string); ok {
				if v == "****" {
					continue
				}
				if len(v) > 256 {
					return fmt.Errorf("stats_token 长度不能超过 256 字符（当前: %d）", len(v))
				}
			}
		case "strm_resolve_mode":
			if v, ok := value.(string); ok {
				mode := strings.ToLower(strings.TrimSpace(v))
				if mode != "auto" && mode != "passthrough" && mode != "always" {
					return fmt.Errorf("strm_resolve_mode 必须是 auto/passthrough/always 之一（当前: %s）", v)
				}
			}
		case "library_scan_incremental_minutes":
			if v := toInt(value); v < 1 || v > 1440 {
				return fmt.Errorf("library_scan_incremental_minutes 必须在 1-1440 分钟之间（当前: %d）", v)
			}
		case "enable_lan_strm", "enable_preload", "enable_smart_ttl", "enable_cdn_warmup", "enable_douban_rating":
			// bool 类型，无额外约束
		}
	}
	return nil
}

func validateListenAddr(addr string) error {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return fmt.Errorf("地址不能为空")
	}
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("地址格式无效（应为 host:port 或 [ipv6]:port）: %w", err)
	}
	_ = host
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("端口不是合法数字: %s", portStr)
	}
	if port < 1 || port > 65535 {
		return fmt.Errorf("端口范围必须在 1-65535 之间（当前: %d）", port)
	}
	return nil
}

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

func toInt(v interface{}) int {
	switch n := v.(type) {
	case int:
		return n
	case float64:
		return int(n)
	case string:
		if parsed, err := strconv.Atoi(n); err == nil {
			return parsed
		}
	}
	return 0
}

func (c *Config) saveConfigToFile() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.saveConfigLocked()
}

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
	viper.Set("strm_resolve_mode", c.StrmResolveMode)
	viper.Set("enable_lan_strm", c.EnableLanStrm)
	viper.Set("enable_preload", c.EnablePreload)
	viper.Set("enable_smart_ttl", c.EnableSmartTTL)
	viper.Set("enable_cdn_warmup", c.EnableCDNWarmup)
	viper.Set("enable_library_scan", c.EnableLibraryScan)
	viper.Set("library_scan_cron", c.LibraryScanCron)
	viper.Set("library_scan_on_start", c.LibraryScanOnStart)
	viper.Set("library_scan_concurrency", c.LibraryScanConcurrency)
	viper.Set("library_scan_interval_ms", c.LibraryScanIntervalMs)
	viper.Set("library_scan_incremental_minutes", c.LibraryScanIncrementalMinutes)
	viper.Set("webhook_notify_token", c.WebhookNotifyToken)
	viper.Set("webhook_notify_delay_seconds", c.WebhookNotifyDelaySeconds)
	viper.Set("enable_douban_rating", c.EnableDoubanRating)
	viper.Set("fnos_username", c.FnosUsername)
	viper.Set("fnos_password", c.FnosPassword)
	// ✅ 观看记录
	viper.Set("play_history_db_path", c.PlayHistoryDBPath)

	dir := filepath.Dir(configFile)
	tmpFile, err := os.CreateTemp(dir, ".fnysfd-config-*.yaml")
	if err != nil {
		if writeErr := viper.WriteConfig(); writeErr != nil {
			return fmt.Errorf("写入配置文件失败: %w", writeErr)
		}
		return nil
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()

	if err := viper.WriteConfigAs(tmpPath); err != nil {
		os.Remove(tmpPath)
		if writeErr := viper.WriteConfig(); writeErr != nil {
			return fmt.Errorf("写入配置文件失败: %w", writeErr)
		}
		return nil
	}

	if err := os.Rename(tmpPath, configFile); err != nil {
		os.Remove(tmpPath)
		if writeErr := viper.WriteConfig(); writeErr != nil {
			return fmt.Errorf("替换配置文件失败（Rename: %v；WriteConfig: %w）", err, writeErr)
		}
		return nil
	}

	log.Printf("💾 配置已保存到: %s", configFile)
	return nil
}

func (c *Config) ToMap() map[string]interface{} {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	dashPassMasked := ""
	if c.DashboardPass != "" {
		dashPassMasked = "****"
	}
	statsTokenMasked := ""
	if c.StatsToken != "" {
		statsTokenMasked = "****"
	}
	webhookTokenMasked := ""
	if c.WebhookNotifyToken != "" {
		webhookTokenMasked = "****"
	}
	fnosPassMasked := ""
	if c.FnosPassword != "" {
		fnosPassMasked = "****"
	}

	return map[string]interface{}{
		"listen":            c.ListenAddr,
		"target":            c.TargetAddr,
		"log_level":         c.LogLevel,
		"log_dir":           c.LogDir,
		"cache_ttl":         int(c.CacheTTL.Minutes()),
		"dashboard_addr":    c.DashboardAddr,
		"dashboard_user":    c.DashboardUser,
		"dashboard_pass":    dashPassMasked,
		"max_cache_items":   c.MaxCacheItems,
		"strm_paths":        c.StrmPaths,
		"strm_volumes":      c.StrmVolumes,
		"stats_token":       statsTokenMasked,
		"strm_resolve_mode": c.StrmResolveMode,
		"enable_lan_strm":   c.EnableLanStrm,
		"enable_preload":    c.EnablePreload,
		"enable_smart_ttl":  c.EnableSmartTTL,
		"enable_cdn_warmup": c.EnableCDNWarmup,
		"enable_library_scan":        c.EnableLibraryScan,
		"library_scan_cron":          c.LibraryScanCron,
		"library_scan_on_start":      c.LibraryScanOnStart,
		"library_scan_concurrency":   c.LibraryScanConcurrency,
		"library_scan_interval_ms":   c.LibraryScanIntervalMs,
		"library_scan_incremental_minutes": c.LibraryScanIncrementalMinutes,
		"webhook_notify_token":         webhookTokenMasked,
		"webhook_notify_delay_seconds": c.WebhookNotifyDelaySeconds,
		"enable_douban_rating": c.EnableDoubanRating,
		"fnos_username": c.FnosUsername,
		"fnos_password": fnosPassMasked,
		// ✅ 观看记录
		"play_history_db_path": c.PlayHistoryDBPath,
	}
}

func (c *Config) SetStrmPaths(paths []string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.StrmPaths = paths
	log.Printf("📁 STRM路径已更新: %v", paths)
	return c.saveConfigLocked()
}

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

	if _, err := os.Stat(containerPath); err == nil {
		log.Printf("✅ 路径在容器内已可访问，无需重建: %s", containerPath)
		return true, nil
	}
	return false, nil
}

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

func (c *Config) GetStrmVolumes() []string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	volumes := make([]string, len(c.StrmVolumes))
	copy(volumes, c.StrmVolumes)
	return volumes
}

func (c *Config) GetStrmPaths() []string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	paths := make([]string, len(c.StrmPaths))
	copy(paths, c.StrmPaths)
	return paths
}

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

func containsString(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

func createDefaultConfig(configPath string) error {
	if configPath == "" {
		configPath = os.Getenv("CONFIG")
		if configPath == "" {
			configPath = "./config.yaml"
		}
	}

	configDir := filepath.Dir(configPath)
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return fmt.Errorf("创建配置目录失败: %w", err)
	}

	defaultConfig := `cache_ttl: 30
dashboard_addr: :28006
dashboard_pass: admin
dashboard_user: admin
enable_lan_strm: true
enable_preload: true
enable_smart_ttl: true
enable_cdn_warmup: true
enable_library_scan: false
library_scan_cron: "03:00"
library_scan_on_start: false
library_scan_concurrency: 2
library_scan_interval_ms: 500
library_scan_incremental_minutes: 5
webhook_notify_token: ""
webhook_notify_delay_seconds: 15
enable_douban_rating: true
fnos_username: ""
fnos_password: ""
play_history_db_path: "/db/trimmedia.db"
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

	volumeLines := ""
	for _, vol := range c.StrmVolumes {
		volumeLines += fmt.Sprintf("      - %s\n", vol)
	}

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
