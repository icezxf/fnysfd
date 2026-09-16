package main

import (
	"fnysfd/internal/config"
	"fnysfd/internal/dashboard"
	"fnysfd/internal/docker"
	"fnysfd/internal/proxy"
	"flag"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

const (
	maxMemoryMB     = 512             // 最大内存使用限制（MB）
	memoryCheckFreq = 5 * time.Minute // 内存检查频率
)

// serverInstance 全局服务器实例
var serverInstance *proxy.Server

// version 通过 -ldflags "-X main.version=xxx" 在构建时注入
var version = "dev"

func main() {
	configPath := os.Getenv("CONFIG")

	var cfgPath string
	flag.StringVar(&cfgPath, "c", "", "配置文件路径")
	flag.Parse()

	if cfgPath != "" {
		configPath = cfgPath
	}

	if err := config.Load(configPath); err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	if err := docker.Init(); err != nil {
		log.Printf("Docker初始化: %v", err)
	}

	if docker.Global != nil {
		docker.Global.SetStrmPaths(config.Global.GetStrmPaths())
		docker.Global.OnRestart = func() {
			log.Println("🔄 容器重建完成，触发进程优雅关闭...")
			if p, err := os.FindProcess(os.Getpid()); err == nil {
				p.Signal(syscall.SIGTERM)
			}
		}
	}

	config.Watch(func() {
		log.Println("🔄 配置已更新")
		if serverInstance != nil {
			serverInstance.Reload()
		}
	})

	server, err := proxy.NewServer(config.Global, version)
	if err != nil {
		log.Fatalf("创建代理服务器失败: %v", err)
	}
	serverInstance = server

	dash := dashboard.New(config.Global, server.GetCache(), server.GetLogger(), version)
	dash.SetStreamHandler(server.GetStreamHandler())
	dash.SetLibraryScanner(server.GetLibraryScanner())
	dash.SetDoubanProvider(server.GetDoubanProvider())
	// ✅ 观看记录服务（服务为 nil 时 dashboard 自动禁用「观看记录」tab）
	if rs := server.GetRecordService(); rs != nil {
		dash.SetRecordProvider(rs)
	}

	log.Printf("🚀 FNYSFD 启动")
	log.Printf("   反代监听: %s", config.Global.GetListenAddr())
	log.Printf("   目标服务: %s", config.Global.GetTargetAddr())
	log.Printf("   管理面板: http://localhost%s", config.Global.GetDashboardAddr())
	log.Printf("   日志级别: %s", config.Global.GetLogLevel())
	log.Printf("   缓存TTL: %v", config.Global.GetCacheTTL())

	memStop := make(chan struct{})
	go memoryMonitor(server, memStop)
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		log.Println("🛑 正在关闭...")
		close(memStop)
		config.Stop()
		dash.Stop()
		server.Stop()
	}()

	if err := dash.Start(); err != nil {
		log.Printf("管理面板启动失败: %v", err)
		_ = server.Stop()
		os.Exit(1)
	}

	if err := server.Start(); err != nil {
		log.Printf("服务器启动失败: %v", err)
		_ = dash.Stop()
		_ = server.Stop()
		os.Exit(1)
	}
}

// memoryMonitor 内存监控协程
func memoryMonitor(s *proxy.Server, stop <-chan struct{}) {
	ticker := time.NewTicker(memoryCheckFreq)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			memMB := float64(m.Alloc) / 1024 / 1024

			if memMB > maxMemoryMB {
				s.GetLogger().Warn("⚠️ 内存使用过高 (%.1fMB/%dMB)，自动清理缓存", memMB, maxMemoryMB)
				s.GetCache().Clear()
				sh := s.GetStreamHandler()
				if sh != nil {
					sh.ClearExternalCaches()
				}
				runtime.GC()

				var newM runtime.MemStats
				runtime.ReadMemStats(&newM)
				newMemMB := float64(newM.Alloc) / 1024 / 1024
				s.GetLogger().Info("✅ 缓存清理完成，内存释放: %.1fMB -> %.1fMB", memMB, newMemMB)
			}
		case <-stop:
			return
		}
	}
}
