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
	ListenAddr    string        `mapstructure:"listen"`
	TargetAddr    string        `mapstructure:"target"`
	LogLevel      string        `mapstructure:"log_level"`
	LogDir        string        `mapstructure:"log_dir"`
	CacheTTL      time.Duration `mapstructure:"cache_ttl"`
	DashboardAddr string        `mapstructure:"dashboard_addr"`
	DashboardUser string        `mapstructure:"dashboard_user"`
	DashboardPass string        `mapstructure:"dashboard_pass"`
	MaxCacheItems int           `mapstructure:"max_cache_items"`
	StrmPaths     []string      `mapstructure:"strm_paths"`
	StrmVolumes   []string      `mapstructure:"strm_volumes"`
	StatsToken    string        `mapstructure:"stats_token"`
	StrmResolveMode string      `mapstructure:"strm_resolve_mode"`
	EnableLanStrm   bool        `mapstructure:"enable_lan_strm"`
	EnablePreload   bool        `mapstructure:"enable_preload"`
	EnableSmartTTL  bool        `mapstructure:"enable_smart_ttl"`
	EnableCDNWarmup bool        `mapstructure:"enable_cdn_warmup"`

	// 全库扫描
	EnableLibraryScan             bool   `mapstructure:"enable_library_scan"`
	LibraryScanCron               string `mapstructure:"library_scan_cron"`
	LibraryScanOnStart            bool   `mapstructure:"library_scan_on_start"`
	LibraryScanConcurrency        int    `mapstructure:"library_scan_concurrency"`
	LibraryScanIntervalMs         int    `mapstructure:"library_scan_interval_ms"`
	LibraryScanIncrementalMinutes int    `mapstructure:"library_scan_incremental_minutes"`

	// Webhook
	WebhookNotifyToken       
