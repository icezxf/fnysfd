# FNYSFD - 飞牛影视反代

> 基于 Go 1.21 开发的飞牛影视透明反代工具，自动解析 .strm 文件并 302 重定向到真实视频地址，内置智能预加载引擎和 CDN 预热加速。

- 适配 **飞牛影视 0.9.3+**
- 支持 夸克网盘、115 网盘、移动 139 云盘、阿里云盘 等生成的 strm
- 已测试 **CapyPlayer**、**Vidhub**、**爆米花** 等播放器正常播放
- **当前版本：v3.3.11**

---

## 功能特性

### 核心反代

- 透明代理飞牛影视服务，拦截 PlaybackInfo 响应并缓存 .strm MediaSource
- 拦截视频流请求，302 重定向到真实 URL（保留 302 不走代理流，适合家庭低带宽场景）
- 15 种流请求模式匹配（stream.mp4、stream.MOV、m3u8 等）
- 支持多种重定向状态码（301 / 302 / 307 / 308）
- 5 种 STRM 路径变体尝试，提升文件查找健壮性

### 智能预加载引擎

- **16 个预加载工作协程**，双优先级队列（电影优先 + 普通队列）
- PlaybackInfo 响应时立即同步预读 STRM，进入详情页即开始解析
- 4 级缓存查找策略：MediaSourceId -> ItemId -> Query 参数 -> 路径扫描
- LRU 双缓存：STRM 文件内容缓存 + URL 解析结果缓存（各 10000 条）
- singleFlight 防止缓存击穿
- 紧急预加载：缓存未命中时为下次请求做准备（信号量限流）

### 三客户端并行极速解析

- **三客户端并行**：fast（2s）+ medium（4s）+ slow（8s）同时跑，谁先成功用谁
- 第一遍解析成功率约 95%
- 10 种 User-Agent 轮换（Chrome/Safari/iPhone/Android/VLC/MPV/Infuse/VidHub）
- 解析失败智能换 UA 重试（绕过源站 UA 限流）
- 负缓存：失败 URL 短期内不再重复尝试

### 下一集智能预取

- PlaybackInfo STRM 缓存成功后自动触发下一集预取
- 查询同季剧集列表，预取后续集的 PlaybackInfo + STRM 解析
- 电影自动跳过下一集预取（避免无意义的 API 调用）
- 基于集数排序 + 列表位置双重定位（兼容 IndexNumber=0 的异常数据）
- 预取链去重（20 秒窗口），避免多触发源并发刷屏

### CDN 预热加速

- **双端预热**：预取文件头 256KB + 异步预取文件尾 256KB（覆盖 MP4 moov atom 位置）
- **MP4 moov atom 智能检测**：faststart 文件跳过尾部预热
- **CDN 预连接**：解析后立即预建 TCP+TLS 连接，省 200-500ms 起播时间
- **起播护航预热**：同步等待 CDN 拿到文件头再返回 302，降低首次 3003 错误
- **预热去重**：60s/30s/5s 差异化 TTL，避免重复预热
- 所有媒体类型（电影/电视剧/动漫/纪录片/综艺）均参与预热

### STRM 解析模式开关

通过 `config.yaml` 的 `strm_resolve_mode` 配置项控制，支持热重载：

| 模式 | 行为 | 适用场景 |
|------|------|---------|
| `auto`（默认） | 智能识别 CDN 直链跳过解析，其他走 HTTP 解析 | 通用场景 |
| `passthrough` | 所有 strm 直接把内容给播放器，不解析/不预热 | 全 302 strm 场景 |
| `always` | 强制所有 strm 走 HTTP 解析 | 调试/回退 |

### 智能 302 直链识别

- 自动判断 strm 文件中的 URL 是否已是直链（已知 CDN 域名、签名参数、路径标记）
- 命中直链场景直接缓存 URL，省去三客户端解析（节省 2-8s）
- 反向排除分享短链（`pan.quark.cn/s/`、`pan.baidu.com/s/` 等）

### 主动预请求机制

- **详情页预请求**：进入详情页即主动预请求 PlaybackInfo，持续重试直到 probe 完成
- **PlaybackInfo 主动预请求**：点击播放时并行发送主动预请求
- **400/403 主动重试**：飞牛首次返回 400/403 时主动重试，提前缓存 MediaSource
- **新剧兜底**：流请求先到但 MediaSource 未缓存时，同步轮询 PlaybackInfo 避免 3003

### 智能签名 TTL 检测

- 自动检测直链 URL 中的签名参数（OSS/阿里云/腾讯云/七牛/华为云等）
- 按签名剩余有效期的 50% 动态设置缓存 TTL（安全余量）
- 确保缓存过期时间始终早于签名过期时间，解决播放中 403 问题

### 管理面板（端口 28006）

- 深色侧边栏 + 浅色内容区，简洁现代 UI
- 概览页：统计卡片 + 系统信息
- 配置页：在线修改 + 热重载，密码字段安全脱敏
- 路径管理：Volume 增删改查 + 路径遍历校验
- 日志页：终端样式 + 语法着色
- Session 自动清理，Cookie 安全标志

### 性能监控

- `/stats` 端点：解析统计、预热统计、预连接统计、缓存大小
- `/health` 健康检查端点
- 支持 token 认证（未配置 token 时仅允许 localhost 访问）

### 运维辅助

- 内存监控（超 512MB 自动清理缓存 + GC）
- 配置文件热重载（2 秒轮询，变更自动触发 Reload）
- 配置原子写入（临时文件 + rename，防止写入中断导致损坏）
- URL 半衰期刷新循环（防止播放中签名 URL 过期）
- Docker 容器管理（compose V2 + 精确容器名匹配）
- 异步日志（按天滚动 + 滚动保留）
- 请求 ID 追踪 + 安全响应头

---

## 快速开始

### Docker Hub 镜像部署

```yaml
# docker-compose.yml
services:
  fnysfd:
    image: jimboo7339/fntv-proxy:latest
    container_name: fnysfd
    restart: unless-stopped
    ports:
      - "28005:28005"
      - "28006:28006"
    volumes:
      - .:/data
      - /vol00:/vol00:ro
    environment:
      - TZ=Asia/Shanghai
    working_dir: /data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:28006/api/system"]
      interval: 30s
      timeout: 3s
      retries: 3
      start_period: 10s
```

```bash
docker compose up -d
```

访问面板：`http://NAS_IP:28006`（默认 admin/admin）

### 本地构建部署

```bash
# 克隆仓库
git clone https://github.com/jimboo7339/fntv-proxy.git
cd fntv-proxy

# Docker 构建
docker build -t fnysfd:3.3.11 .

# 或直接编译
go build -ldflags="-s -w -X main.version=3.3.11" -o fnysfd ./cmd
```

### 使用步骤

1. 启动服务后访问管理面板 `http://NAS_IP:28006`
2. 在「路径管理」中添加 STRM 文件所在的主机路径和容器路径映射
3. 在飞牛影视中将播放地址指向 `http://NAS_IP:28005`
4. 播放视频即可享受加速

---

## 配置说明

`config.yaml` 示例：

```yaml
# 反代监听地址
listen: ":28005"

# 飞牛影视服务地址
target: "http://127.0.0.1:8005"

# 日志级别: trace / debug / info / warn / error
log_level: "info"

# 日志目录
log_dir: "./logs"

# 直链缓存过期时间（分钟），默认 30 分钟
cache_ttl: 30

# 最大缓存条目数
max_cache_items: 10000

# 管理面板
dashboard_addr: ":28006"
dashboard_user: "admin"
dashboard_pass: "admin"

# /stats 端点认证 token（留空则仅 localhost 可访问）
stats_token: ""

# STRM 解析模式: auto / passthrough / always
strm_resolve_mode: "auto"

# STRM 文件路径列表（容器内路径）
strm_paths:
  - /vol00

# Docker Volume 映射（格式：/host:/container:ro）
strm_volumes:
  - /vol00:/vol00:ro

# 功能开关（默认全部打开）
enable_lan_strm: true    # 内网strm地址支持
enable_preload: true     # 预加载
enable_smart_ttl: true   # 智能签名TTL检测
enable_cdn_warmup: true  # CDN预热
```

### 环境变量

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `CONFIG` | 配置文件路径 | `./config.yaml` |
| `TZ` | 时区 | `Asia/Shanghai` |
| `PROXY_PORT` | 反代端口 | `28005` |
| `DASHBOARD_PORT` | 面板端口 | `28006` |
| `TARGET_URL` | 飞牛影视地址 | `http://127.0.0.1:8005` |

---

## 端口说明

| 端口 | 用途 |
|------|------|
| `28005` | 反代服务端口（指向飞牛影视） |
| `28006` | 管理面板端口（Web UI） |
| `/stats` | 性能监控 JSON（28005） |
| `/health` | 健康检查端点（28005） |

---

## 工作原理

```
用户进入详情页 -> 代理拦截 /Items/{id} -> 主动预请求 PlaybackInfo
                                              |
                                              v
飞牛 probe 中 (返回 400) -> 持续重试 -> probe 完成 (返回 200)
                                              |
                                              v
PlaybackInfo 响应处理 -> 同步预读 STRM -> 缓存直链 URL
                                              |
                    +-------------------------+-------------------------+
                    |                                                   |
                    v                                                   v
          异步预加载队列兜底                                    下一集预取（仅剧集）
          (16 worker, 双优先级)                                 查询同季列表 -> 预取后续集
                    |
                    v
          CDN 预连接 (TCP+TLS) -> CDN 预热 (256KB 文件头)
                    |
                    v
播放器 -> stream.mp4 -> 代理 4 级查找 -> 302 重定向 -> CDN 播放
  |                                   |
  +-- MediaSourceId 缓存命中 (零延迟)  |
  +-- ItemId 查找命中                  |
  +-- Query 参数命中                   |
  +-- 路径扫描命中                     |
  +-- 未命中: 转发 + 紧急预加载 --------+
```

---

## 目录结构

```
fntv-proxy/
├── cmd/
│   └── main.go                 # 入口（内存监控 + 热重载触发）
├── internal/
│   ├── config/
│   │   └── config.go           # 配置管理（热重载 + 原子写入 + 校验）
│   ├── proxy/
│   │   └── server.go           # 代理服务器 + 下一集预取 + 主动预请求
│   ├── handler/
│   │   ├── playback.go         # PlaybackInfo 处理 + STRM 缓存 + 预加载触发
│   │   └── stream.go           # 流处理 + 智能预加载引擎 + CDN 预热
│   ├── cache/
│   │   └── cache.go            # 缓存（LRU + TTL + singleFlight）
│   ├── dashboard/
│   │   ├── dashboard.go        # 面板 API（Session 清理 + 路径校验）
│   │   └── templates.go        # 前端 UI
│   ├── docker/
│   │   └── docker_manager.go   # 容器管理（compose V2）
│   └── logger/
│       └── logger.go           # 异步日志（原子级别 + 滚动保留）
├── .github/workflows/
│   └── docker-image.yml        # CI/CD: Docker 镜像自动构建推送
├── config.yaml                 # 默认配置文件
├── docker-compose.yml          # Docker Compose 部署
├── Dockerfile                  # 多阶段构建（支持 amd64 + arm64）
├── build-docker.sh             # Docker 构建脚本
├── entrypoint.sh               # 容器启动脚本
└── README.md
```

---

## 常见问题

### Q: 支持哪些视频格式？
支持 `stream.mp4`、`stream.MOV`、`.mkv`、`.avi`、`.mov`、`.flv`、`.wmv`、`.ts`、`.m4s`、`master.m3u8` 等所有格式。

### Q: 缓存多久过期？
默认 30 分钟，可通过 `cache_ttl` 配置。启用智能 TTL 后，签名 URL 会按签名有效期动态设置更短的 TTL。

### Q: 起播报 3003 错误怎么办？
3003 通常是因为 CDN 未预热完成。v3.3.11 已修复电影跳过预热的问题，确保所有媒体类型都参与 CDN 预热。如果仍有问题，尝试返回详情页重新进入。

### Q: 如何查看详细日志？
修改 `log_level: debug` 或 `trace`，日志自动写入 `./logs` 目录。

### Q: 配置修改后需要重启吗？
大部分配置支持热重载。仅以下配置变更需要重启：
- `listen`（监听端口）
- `dashboard_addr`（面板端口）
- `strm_volumes`（Volume 映射）

### Q: strm 路径一定要挂载到 Docker 容器中吗？
是的，否则会播放失败，找不到 strm 路径。

### Q: 电影和电视剧的处理有什么区别？
v3.3.11 起，电影和电视剧都参与完整的预加载和 CDN 预热流程。唯一区别是电影不会触发下一集预取（因为电影没有下一集）。

---

## 版本更新日志

### v3.3.11 - 电影预热修复版

- **修复电影起播 3003**：移除 7 处电影跳过预热的代码路径，电影现在与电视剧一样完整参与预加载、CDN 预连接和预热
- **电影下一集预取跳过**：电影播放时不触发下一集预取回调，避免无意义的 API 调用

### v3.3.10 - 媒体类型识别版

- 新增 Type 字段检查，非剧集类型（电影/纪录片等）自动跳过下一集预取
- 非剧集跳过日志升级为 Info 级别

### v3.3.8 - v3.3.9 - FNOS 兼容修复

- 修复 FNOS 拦截 `/emby/Items/{id}` 返回 HTML 的问题，改用 `/emby/Users/{userId}/Items/{id}`
- 调整剧集列表查询优先级，避免 HTML 响应

### v3.3.5 - v3.3.7 - 预取链修复

- 修复 allStrmCached 早返回阻断预取回调的问题
- 修复 gzip 压缩导致 JSON 解析失败
- 预取失败日志升级为 Info/Warn 级别

### v3.3.2 - v3.3.4 - 事件风暴 + 预取修复

- 修复下一集预取事件风暴（per-itemID 处理锁）
- 修复 IndexNumber=0 导致后续集被跳过的问题
- 修复预取链断裂（预取回调不触发）

### v3.3.1 - 路径映射热更新版

- Web 面板路径管理，Volume 增删改查
- 无需重建容器即可添加 STRM 路径

### v3.2 - 起播护航 + URL 校验并行

- 起播护航预热 3s 超时 + 5s 去重，解决切集 3003
- URL 校验与 CDN 预热并行执行，起播速度提升约 900ms
- 全格式媒体检测、日志显示剧集名称

### v2.4.1 - 智能签名 TTL + 功能开关

- 智能签名 TTL 检测，解决播放 30 分钟后 403
- 内网 strm 支持开关、预加载开关

---

## 声明

1. 本项目主要针对 **夸克网盘** 在 **openlist** 的夸克TV驱动挂载下实现 302；也支持 openlist、litepan 等工具生成的 302 strm，以及移动 139 云盘直链
2. 只要 strm 文件中的地址能正常下载文件，就可以通过本工具实现第三方播放器播放
3. 经测试 CapyPlayer、Vidhub、爆米花 播放器正常播放

---

## 贡献者

<a href="https://github.com/jimboo7339/fntv-proxy/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=jimboo7339/fntv-proxy" />
</a>

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=jimboo7339/fntv-proxy&type=Date)](https://star-history.com/#jimboo7339/fntv-proxy&Date)
