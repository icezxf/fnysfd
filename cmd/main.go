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
	maxMemoryMB     = 512  // 最大内存使用限制（MB）
	memoryCheckFreq = 5 * time.Minute // 内存检查频率
)

// serverInstance 全局服务器实例
// 注意：本变量赋值在 config.Watch 注册之前完成，Reload 回调中的读取依赖
// Go 1.19+ 对齐指针读写的原子性保证。若未来需要在运行时替换实例，
// 应改用 atomic.Pointer[proxy.Server]。
var serverInstance *proxy.Server

// version 通过 -ldflags "-X main.version=xxx" 在构建时注入
// 默认值 "dev" 用于开发环境
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

	// 设置 Docker 重建完成后的优雅关闭回调（替代 docker_manager 中的裸 os.Exit）
	// 容器重建成功后，UpdateStrmPaths 会异步调用 OnRestart，
	// 这里向自己发送 SIGTERM 复用下方信号处理逻辑完成优雅退出
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
		// 触发代理服务器热重载，使新配置（目标地址/日志级别/缓存TTL等）生效
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
		close(memStop) // 通知内存监控协程退出
		config.Stop()  // 停止配置文件监听
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
// stop 用于在服务关闭时通知本协程退出，避免泄漏
func memoryMonitor(s *proxy.Server, stop <-chan struct{}) {
	ticker := time.NewTicker(memoryCheckFreq)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			// 使用 Alloc（当前活跃分配）而非 Sys（历史总申请），更准确反映实际占用
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
