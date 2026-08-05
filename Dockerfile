# v3.4.0 - 飞牛影视反代 (STRM专用优化 + 媒体信息预取)
# =================================================================

# 阶段1: 编译Go程序
FROM golang:1.21-alpine AS builder

WORKDIR /app

# 安装依赖
RUN apk add --no-cache git ca-certificates tzdata

# 设置Go模块代理（使用国内镜像加速，支持 fallback）
ENV GOPROXY=https://goproxy.cn,https://proxy.golang.org,direct
ENV GO111MODULE=on

# 复制源代码
COPY . .

# 下载并校验 Go 依赖
RUN go mod download

# 编译参数：静态链接、去除调试信息、优化大小
# 支持多平台构建（amd64 + arm64），由 buildx 注入 TARGETARCH
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build \
    -ldflags="-s -w -X main.version=3.4.0" \
    -o fnysfd-linux \
    ./cmd

# 阶段2: 运行时镜像（轻量级Alpine）
FROM alpine:latest

LABEL maintainer="FNYSFD"
LABEL description="飞牛影视反代服务 + 管理面板 (STRM专用优化 + 媒体信息预取)"
LABEL version="3.4.0"

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

# 健康检查
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:28006/api/system || exit 1

# 启动命令
ENTRYPOINT ["/app/entrypoint.sh"]
