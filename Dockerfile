# v3.4.0 - 飞牛影视反代 (STRM专用优化 + 媒体信息预取)
# 仅支持 ARM64 架构
# =================================================================

# 阶段1: 编译Go程序
FROM --platform=linux/arm64 golang:1.21-alpine AS builder

WORKDIR /app

# 安装依赖
RUN apk add --no-cache git ca-certificates tzdata

# 设置Go模块代理（使用国内镜像加速，支持 fallback）
ENV GOPROXY=https://goproxy.cn,https://proxy.golang.org,direct
ENV GO111MODULE=on

# 复制源代码
COPY . .

# ✅ 新增：拉取 modernc.org/sqlite 依赖（观看记录功能需要）
#    因为仓库里的 go.mod 还没包含这个依赖，构建时动态加入
RUN go get modernc.org/sqlite@v1.34.5
RUN go mod tidy

# 下载并校验 Go 依赖
RUN go mod download

# 编译参数：静态链接、去除调试信息、优化大小
# 固定为 ARM64 架构
ENV GOARCH=arm64
ENV GOOS=linux
RUN CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build \
    -ldflags="-s -w -X main.version=3.4.0" \
    -o fnysfd-linux \
    ./cmd

# 阶段2: 运行时镜像（轻量级Alpine ARM64）
FROM --platform=linux/arm64 alpine:latest

LABEL maintainer="FNYSFD"
LABEL description="飞牛影视反代服务 + 管理面板 (STRM专用优化 + 媒体信息预取) - ARM64"
LABEL version="3.4.0"
LABEL architecture="arm64"

WORKDIR /app

# 安装运行时依赖
RUN apk --no-cache add ca-certificates tzdata curl && \
    rm -rf /var/cache/apk/*

# 设置时区
ENV TZ=Asia/Shanghai
RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    echo "Asia/Shanghai" > /etc/timezone

# 创建必要目录
RUN mkdir -p /app/logs /app/config

# 从编译阶段复制二进制文件
COPY --from=builder /app/fnysfd-linux /app/fnysfd
COPY entrypoint.sh /app/entrypoint.sh

# 设置执行权限
RUN chmod +x /app/fnysfd /app/entrypoint.sh

# 暴露端口
EXPOSE 28005 28006

# ✅ 修正：健康检查走 /health（28005，不需要登录）
#    原版走 28006/api/system 会因为需要 session 而永远 401
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -fsS http://localhost:28005/health || exit 1

# 启动命令
ENTRYPOINT ["/app/entrypoint.sh"]
