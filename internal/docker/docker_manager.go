package docker

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

type Manager struct {
	containerName string
	composeFile    string
	enabled       bool
	// OnRestart 在容器重建成功后由 UpdateStrmPaths 异步触发，
	// 上层（main.go）可设置此回调以优雅关闭当前进程（替代裸 os.Exit）
	OnRestart func()
	// strmPaths 用于过滤 /proc/mounts 检测到的挂载点，避免启发式过宽
	strmPaths []string
}

var Global *Manager

// composeCmdOnce / cachedComposeCmd 用于缓存 composeCommand 的探测结果
var (
	composeCmdOnce   sync.Once
	cachedComposeCmd []string
)

// composeCommand 返回 docker compose 命令切片（优先 V2 插件，fallback 到 V1 docker-compose）
// 返回值例如 ["docker", "compose"] 或 ["docker-compose"]
func composeCommand() []string {
	composeCmdOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// 优先尝试 docker compose V2 子命令
		if err := exec.CommandContext(ctx, "docker", "compose", "version").Run(); err == nil {
			cachedComposeCmd = []string{"docker", "compose"}
			return
		}
		// fallback 到 V1 docker-compose
		cachedComposeCmd = []string{"docker-compose"}
	})
	return cachedComposeCmd
}

// SetStrmPaths 设置STRM路径（用于过滤 /proc/mounts 挂载点检测）
func (m *Manager) SetStrmPaths(paths []string) {
	m.strmPaths = make([]string, len(paths))
	copy(m.strmPaths, paths)
}

func Init() error {
	composeFile := os.Getenv("COMPOSE_FILE")
	if composeFile == "" {
		composeFile = "docker-compose.yml"
	}

	containerName := os.Getenv("CONTAINER_NAME")
	if containerName == "" {
		containerName = "fnysfd"
	}

	// 使用 docker info 探活 daemon（比 docker --version 更可靠，
	// --version 只校验二进制存在，无法反映 daemon 是否可用）
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "info")
	if err := cmd.Run(); err != nil {
		Global = &Manager{enabled: false}
		return nil
	}

	Global = &Manager{
		containerName: containerName,
		composeFile:    composeFile,
		enabled:       true,
	}

	return nil
}

func (m *Manager) IsEnabled() bool {
	return m.enabled
}

// UpdateStrmPaths 重建容器以应用新的STRM路径（已废弃，请使用 RestartContainer）
// 通过 OnRestart 回调通知上层优雅关闭，避免裸 os.Exit(0) 导致资源未释放
func (m *Manager) UpdateStrmPaths(newPaths []string) error {
	return m.RestartContainer()
}

// RestartContainer 一键重启容器（docker compose restart）
// 比 down + up 更快，且不会删除容器，用于应用新的 volume 配置
func (m *Manager) RestartContainer() error {
	if !m.IsEnabled() {
		return fmt.Errorf("Docker 管理器未启用")
	}

	composeCmd := composeCommand()
	// 60 秒超时，避免命令卡死
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	restartArgs := append(composeCmd[1:], "-f", m.composeFile, "restart")
	cmd := exec.CommandContext(ctx, composeCmd[0], restartArgs...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("重启容器失败: %w", err)
	}

	log.Println("✅ 容器已重启，新 Volume 配置已生效")

	// 通过回调通知上层优雅关闭当前进程（因为容器重启后旧进程会被新容器取代）
	if m.OnRestart != nil {
		go func() {
			time.Sleep(2 * time.Second)
			m.OnRestart()
		}()
	}

	return nil
}

func (m *Manager) GetContainerInfo() map[string]interface{} {
	if !m.IsEnabled() {
		return map[string]interface{}{
			"enabled": false,
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// 用 ^name$ 锚定容器名，避免子串匹配（如 fnysfd 匹配到 fnysfd-helper）
	cmd := exec.CommandContext(ctx, "docker", "ps", "--filter", fmt.Sprintf("name=^%s$", m.containerName), "--format", "{{.ID}}\t{{.Status}}\t{{.Ports}}")
	output, err := cmd.Output()
	if err != nil {
		return map[string]interface{}{
			"enabled":  true,
			"error":   err.Error(),
		}
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	info := make(map[string]interface{})
	info["enabled"] = true
	info["container_name"] = m.containerName

	if len(lines) > 0 && lines[0] != "" {
		parts := strings.Split(lines[0], "\t")
		if len(parts) >= 1 {
			info["container_id"] = parts[0]
		}
		if len(parts) >= 2 {
			info["status"] = parts[1]
		}
		if len(parts) >= 3 {
			info["ports"] = parts[2]
		}

		containerID := parts[0]
		mounts := m.GetContainerMounts(containerID)
		info["mounts"] = mounts
	} else {
		info["status"] = "not running"
		info["mounts"] = []string{}
	}

	return info
}

// GetContainerMounts 获取容器的实际挂载点（从Docker运行时读取）
// 使用 m.strmPaths 做交集过滤（若已通过 SetStrmPaths 设置）
func (m *Manager) GetContainerMounts(containerID string) []string {
	if !m.IsEnabled() {
		return []string{}
	}

	mounts := []string{}

	method1 := m.getMountsFromProc()
	if len(method1) > 0 {
		mounts = append(mounts, method1...)
	}

	if containerID != "" {
		method2 := m.getMountsFromDockerInspect(containerID)
		mounts = append(mounts, method2...)
	}

	uniqueMounts := []string{}
	seen := make(map[string]bool)
	for _, mount := range mounts {
		if !seen[mount] {
			seen[mount] = true
			uniqueMounts = append(uniqueMounts, mount)
		}
	}

	return uniqueMounts
}

// pathMatchesStrmPaths 判断 dest 是否与配置的 strmPaths 有交集
// 交集规则：dest 等于某个 strmPath，或以 strmPath + "/" 为前缀（即 dest 在该路径下）
func pathMatchesStrmPaths(dest string, strmPaths []string) bool {
	for _, p := range strmPaths {
		if p == "" {
			continue
		}
		if dest == p {
			return true
		}
		// 前缀匹配（避免 /vol0 误匹配 /vol00）
		if strings.HasPrefix(dest, p) && strings.HasPrefix(strings.TrimPrefix(dest, p), "/") {
			return true
		}
	}
	return false
}

// getMountsFromProc 从 /proc/mounts 获取挂载点（最可靠的方式）
// 若 m.strmPaths 非空，则只保留与 strmPaths 有交集的挂载；否则退回原有启发式
func (m *Manager) getMountsFromProc() []string {
	data, err := os.ReadFile("/proc/mounts")
	if err != nil {
		log.Printf("⚠️ 无法读取 /proc/mounts: %v", err)
		return []string{}
	}

	mounts := []string{}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}

		dest := fields[1]
		fstype := fields[2]
		opts := fields[3]

		// 过滤策略：优先使用 strmPaths 做精确交集；未设置时退回启发式
		if len(m.strmPaths) > 0 {
			if !pathMatchesStrmPaths(dest, m.strmPaths) {
				continue
			}
		} else {
			isStrmPath := (strings.HasPrefix(dest, "/vol") && dest != "/") ||
				(strings.HasPrefix(dest, "/media")) ||
				(strings.Contains(dest, "strm")) ||
				(strings.Contains(dest, "Media"))
			if !isStrmPath {
				continue
			}
		}

		mode := "rw"
		if strings.Contains(opts, "ro") && !strings.Contains(opts, "rw") {
			mode = "ro"
		}

		var displaySource string
		if fstype == "overlay" || fstype == "9p" || fstype == "bind" {
			displaySource = dest
		} else {
			displaySource = fields[0]
		}

		volumeFormat := displaySource + ":" + dest + ":" + mode
		mounts = append(mounts, volumeFormat)
		log.Printf("🔍 检测到挂载: %s (类型: %s)", volumeFormat, fstype)
	}

	log.Printf("📊 从 /proc/mounts 获取到 %d 个STRM相关挂载", len(mounts))
	return mounts
}

// getMountsFromDockerInspect 使用 docker inspect 获取挂载点（备用方案）
// 模板使用 {{println}} 输出换行（避免 {{println ""}} 产生额外空字符）
func (m *Manager) getMountsFromDockerInspect(containerID string) []string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "inspect", containerID, "--format", "{{range .Mounts}}{{.Source}}:{{.Destination}}:{{.Mode}}{{println}}{{end}}")
	output, err := cmd.Output()
	if err != nil {
		log.Printf("⚠️ docker inspect 获取挂载点失败: %v", err)
		return []string{}
	}

	mounts := []string{}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" && strings.Contains(line, ":") {
			mounts = append(mounts, line)
		}
	}

	return mounts
}
