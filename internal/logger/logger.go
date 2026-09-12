package logger

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxLogFileSize     = 10 * 1024 * 1024 // 单个日志文件最大10MB
	logRetentionDays   = 1                // ✅ 日志保留天数（只保留当天）
	logCleanupInterval = 6 * time.Hour    // ✅ 每6小时清理一次
)

// Level 日志级别
type Level int

const (
	TraceLevel Level = iota // 新增：最详细的追踪级别
	DebugLevel
	InfoLevel
	WarnLevel
	ErrorLevel
)

// Logger 日志记录器
type Logger struct {
	level      atomic.Int32
	logDir     string
	consoleLog bool // 是否输出到控制台
	fileLog    bool // 是否输出到文件
	mutex      sync.Mutex

	fileHandle      *os.File
	bufWriter       *bufio.Writer
	currentDate     string
	currentFileSize int64 // 当前文件大小
	logChan         chan logEntry
	shutdown        chan struct{}
	wg              sync.WaitGroup

	closeOnce sync.Once   // 保证 Close 只执行一次（防 close of closed channel panic）
	closed    atomic.Bool // 标记是否已关闭，log() 检查后直接 return 避免 drain 后继续入队丢日志
}

type logEntry struct {
	level   string
	message string
	console bool
	ts      time.Time // 入队时间，异步延迟大时仍能反映真实日志时间
}

// New 创建日志记录器
// level: debug, info, warn, error
// logDir: 日志目录，空字符串表示不写文件
func New(level, logDir string) *Logger {
	l := &Logger{
		logDir:     logDir,
		consoleLog: true,
		fileLog:    logDir != "",
		logChan:    make(chan logEntry, 10000),
		shutdown:   make(chan struct{}),
	}
	l.level.Store(int32(parseLevel(level)))

	if l.fileLog {
		if err := os.MkdirAll(logDir, 0755); err != nil {
			log.Printf("⚠️ 创建日志目录失败: %v", err)
			l.fileLog = false
		} else {
			l.initLogFile()
		}
	}

	// 启动异步写入协程（统一处理控制台与文件输出，避免 hot path 同步阻塞）
	l.wg.Add(1)
	go l.asyncWriter()

	if l.fileLog {
		// 定期清理旧日志，纳入 wg 以便优雅关闭
		l.wg.Add(1)
		go l.cleanupOldLogs()
	}

	return l
}

// cleanupOldLogs 定期清理旧日志
//
// ✅ 改进：
//  1. 启动时先执行一次清理（避免刚重启时旧日志堆积）
//  2. 清理间隔从 24h 改为 6h（更及时）
//  3. 清理逻辑从"按修改时间"改为"按文件名日期"（更精确）
func (l *Logger) cleanupOldLogs() {
	defer l.wg.Done()

	// ✅ 启动时先执行一次清理
	l.doCleanup()

	ticker := time.NewTicker(logCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			l.doCleanup()
		case <-l.shutdown:
			return
		}
	}
}

// doCleanup 执行清理操作
//
// ✅ 改为按文件名日期过滤：只保留今天的日志（含轮转 _1/_2/_3）
// 避免"修改时间"因文件复制/系统时间调整导致的误删/误留
func (l *Logger) doCleanup() {
	if l.logDir == "" {
		return
	}

	entries, err := os.ReadDir(l.logDir)
	if err != nil {
		return
	}

	// 计算保留的最早日期（今天 - (retentionDays-1)）
	today := time.Now()
	cutoffDay := today.AddDate(0, 0, -(logRetentionDays - 1)).Format("20060102")

	removed := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		// 解析文件名开头的日期部分（格式 YYYYMMDD）
		datePart := name
		if idx := strings.Index(name, "_"); idx > 0 {
			datePart = name[:idx]
		}
		datePart = strings.TrimSuffix(datePart, ".log")
		if len(datePart) != 8 {
			continue
		}
		// 文件名日期 < 保留截止日期 → 删除
		if datePart < cutoffDay {
			path := filepath.Join(l.logDir, name)
			if err := os.Remove(path); err == nil {
				removed++
			}
		}
	}

	if removed > 0 {
		log.Printf("🧹 [日志清理] 已删除 %d 个超过 %d 天的日志文件", removed, logRetentionDays)
	}
}

// initLogFile 加锁版本初始化日志文件
func (l *Logger) initLogFile() {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.initLogFileLocked()
}

// initLogFileLocked 初始化日志文件（调用方必须已持有 l.mutex，避免重入死锁）
func (l *Logger) initLogFileLocked() {
	today := time.Now().Format("20060102")
	filename := filepath.Join(l.logDir, today+".log")

	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("⚠️ 打开日志文件失败: %v", err)
		return
	}

	info, statErr := f.Stat()
	if statErr == nil {
		l.currentFileSize = info.Size()
	}

	l.fileHandle = f
	l.bufWriter = bufio.NewWriterSize(f, 64*1024) // 64KB buffer
	l.currentDate = today
}

// ensureLogFileLocked 检查并切换到当天的日志文件（调用方必须已持有 l.mutex）
// 改为 Locked 版本是为了避免对 bufWriter/fileHandle/currentDate 的无锁访问竞态
func (l *Logger) ensureLogFileLocked() {
	today := time.Now().Format("20060102")

	if l.currentDate != today {
		if l.bufWriter != nil {
			l.bufWriter.Flush()
		}
		if l.fileHandle != nil {
			l.fileHandle.Close()
		}
		// 已持有锁，调用 Locked 版本避免重入死锁
		l.initLogFileLocked()
	}
}

// asyncWriter 异步写入日志（统一处理控制台与文件输出）
func (l *Logger) asyncWriter() {
	defer l.wg.Done()

	// processEntry 处理单条日志：控制台 + 文件
	// 时间戳取入队时间 entry.ts，避免异步延迟导致时间不准
	processEntry := func(entry logEntry) {
		if entry.console && l.consoleLog {
			timestamp := entry.ts.Format("2006-01-02 15:04:05")
			logLine := fmt.Sprintf("[%s] %s: %s", timestamp, entry.level, entry.message)
			log.Println(logLine)
		}
		if l.fileLog {
			l.writeToFile(entry)
		}
	}

	for {
		select {
		case entry := <-l.logChan:
			processEntry(entry)
		case <-l.shutdown:
			// 关闭前 drain 残余日志，避免丢失
			for {
				select {
				case entry := <-l.logChan:
					processEntry(entry)
				default:
					if l.bufWriter != nil {
						l.bufWriter.Flush()
					}
					if l.fileHandle != nil {
						l.fileHandle.Close()
					}
					return
				}
			}
		}
	}
}

func (l *Logger) writeToFile(entry logEntry) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// 在锁内检查并切换日志文件，避免对共享字段的无锁访问
	l.ensureLogFileLocked()

	if l.bufWriter == nil || l.fileHandle == nil {
		return
	}

	// 时间戳取入队时间，保证异步场景下时间准确
	timestamp := entry.ts.Format("2006-01-02 15:04:05")
	logLine := fmt.Sprintf("[%s] %s: %s\n", timestamp, entry.level, entry.message)

	if _, err := l.bufWriter.WriteString(logLine); err != nil {
		log.Printf("❌ 写入日志失败: %v", err)
		return
	}

	l.currentFileSize += int64(len(logLine))

	if l.currentFileSize > maxLogFileSize {
		l.bufWriter.Flush()
		l.fileHandle.Close()
		l.rotateLogFile()
		// 已持有锁，调用不加锁版本避免重入死锁
		l.initLogFileLocked()
	}

	// ✅ 所有级别都立即 Flush，保证日志实时可见
	// Flush 只是把 Go bufWriter 写入 OS page cache，开销极小；
	// Sync 才是真正的强制落盘，只对 ERROR/WARN 做。
	l.bufWriter.Flush()
	if entry.level == "ERROR" || entry.level == "WARN" {
		if l.fileHandle != nil {
			l.fileHandle.Sync()
		}
	}
}

// rotateLogFile 日志轮转（当文件过大时），滚动保留 _1, _2, _3
// 调用方必须已持有 l.mutex
func (l *Logger) rotateLogFile() {
	if l.currentDate == "" {
		return
	}

	base := filepath.Join(l.logDir, l.currentDate)
	// 删除最老的 _3
	os.Remove(base + "_3.log")
	// 依次滚动：_2 -> _3, _1 -> _2
	for i := 2; i >= 1; i-- {
		src := fmt.Sprintf("%s_%d.log", base, i)
		dst := fmt.Sprintf("%s_%d.log", base, i+1)
		if _, err := os.Stat(src); err == nil {
			if err := os.Rename(src, dst); err != nil {
				// Rename 失败记录警告但不中断，后续仍尝试写入原文件
				log.Printf("⚠️ 日志轮转 Rename 失败 %s -> %s: %v", src, dst, err)
			}
		}
	}
	// 当前文件 -> _1
	if err := os.Rename(base+".log", base+"_1.log"); err != nil {
		// 当前文件 Rename 失败：记录警告，继续写入原文件（initLogFileLocked 会以 O_APPEND|O_CREATE 重开）
		log.Printf("⚠️ 日志轮转 Rename 当前文件失败 %s -> %s: %v，将继续写入原文件",
			base+".log", base+"_1.log", err)
	}
}

func parseLevel(level string) Level {
	switch level {
	case "trace":
		return TraceLevel
	case "debug":
		return DebugLevel
	case "info":
		return InfoLevel
	case "warn":
		return WarnLevel
	case "error":
		return ErrorLevel
	default:
		return InfoLevel
	}
}

// SetLevel 设置日志级别
func (l *Logger) SetLevel(level string) {
	l.level.Store(int32(parseLevel(level)))
}

// GetLevel 获取当前日志级别
func (l *Logger) GetLevel() Level {
	return Level(l.level.Load())
}

// Close 关闭日志记录器
// 用 sync.Once 包裹 close(shutdown)，防重复调用 panic；
// 关闭前先置 closed=true，让 log() 不再入队，减少 drain 后丢日志的竞态
func (l *Logger) Close() {
	l.closeOnce.Do(func() {
		l.closed.Store(true)
		close(l.shutdown)
	})
	l.wg.Wait()
}

// Trace 追踪日志（最详细，记录请求/响应完整内容）
// fileLog=false 时 fallback 到 console，避免 Trace 日志完全丢失
func (l *Logger) Trace(format string, v ...interface{}) {
	if Level(l.level.Load()) <= TraceLevel {
		msg := fmt.Sprintf(format, v...)
		l.log("TRACE", msg, !l.fileLog)
	}
}

// Debug 调试日志
// fileLog=false 时 fallback 到 console，避免 Debug 日志完全丢失
func (l *Logger) Debug(format string, v ...interface{}) {
	if Level(l.level.Load()) <= DebugLevel {
		msg := fmt.Sprintf(format, v...)
		l.log("DEBUG", msg, !l.fileLog)
	}
}

// Info 信息日志
func (l *Logger) Info(format string, v ...interface{}) {
	if Level(l.level.Load()) <= InfoLevel {
		msg := fmt.Sprintf(format, v...)
		l.log("INFO", msg, true)
	}
}

// Warn 警告日志
func (l *Logger) Warn(format string, v ...interface{}) {
	if Level(l.level.Load()) <= WarnLevel {
		msg := fmt.Sprintf(format, v...)
		l.log("WARN", msg, true)
	}
}

// Error 错误日志
func (l *Logger) Error(format string, v ...interface{}) {
	if Level(l.level.Load()) <= ErrorLevel {
		msg := fmt.Sprintf(format, v...)
		l.log("ERROR", msg, true)
	}
}

// log 统一日志入口（异步非阻塞）
func (l *Logger) log(level, message string, console bool) {
	// 已关闭：直接 return，避免 Close drain 完成后仍向 logChan 入队导致丢日志
	if l.closed.Load() {
		return
	}
	// 控制台与文件输出统一丢进 logChannel，避免 hot path 同步阻塞
	if l.fileLog || (console && l.consoleLog) {
		// 入队时记录时间，异步延迟大时仍能反映真实日志时间
		entry := logEntry{level: level, message: message, console: console, ts: time.Now()}
		select {
		case l.logChan <- entry:
		default:
			log.Println("⚠️ 日志队列已满，丢弃日志:", message)
		}
	}
}
