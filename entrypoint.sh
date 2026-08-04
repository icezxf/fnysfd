#!/bin/sh

# FNYSFD 启动脚本 v3.3.11
# STRM专用优化版 + Compose自动更新（原位置）
# ===========================================

set -e

echo "╔══════════════════════════════════════╗"
echo "║     🚀 FNYSFD 零配置启动脚本          ║"
echo "║     版本: 3.3.11 (STRM专用优化)    ║"
echo "╚══════════════════════════════════════╝"
echo ""

PROXY_PORT="${PROXY_PORT:-28005}"
DASHBOARD_PORT="${DASHBOARD_PORT:-28006}"
TARGET_URL="${TARGET_URL:-http://127.0.0.1:8005}"
CONFIG_DIR="/data/fnysfd-config"
CONFIG_FILE="${CONFIG_DIR}/config.yaml"

mkdir -p "$CONFIG_DIR"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "📝 首次启动：生成默认配置..."
    cat > "$CONFIG_FILE" << EOF
listen: ":${PROXY_PORT}"
target: "${TARGET_URL}"
dashboard_addr: ":${DASHBOARD_PORT}"
dashboard_user: "admin"
dashboard_pass: "admin"
log_level: info
cache_ttl: 30
strm_volumes: []
EOF
    echo "✅ 默认配置已创建: $CONFIG_FILE"
else
    echo "📂 使用已有配置: $CONFIG_FILE"
fi

echo ""
echo "🌐 服务信息:"
echo "   反代端口: ${PROXY_PORT}"
echo "   面板端口: ${DASHBOARD_PORT}"
echo "   目标服务: ${TARGET_URL}"
echo ""
echo "💾 配置位置: ${CONFIG_FILE}"
echo ""
echo "📁 工作目录: /data (对应宿主机Compose项目目录)"
echo ""

if [ -f "$CONFIG_FILE" ]; then
    STRM_VOLUMES=$(grep -A 100 '^strm_volumes:' "$CONFIG_FILE" | grep '^    - ' | sed 's/^    - //' | tr '\n' '|' | sed 's/|$//')

    if [ -n "$STRM_VOLUMES" ] && [ "$STRM_VOLUMES" != "[]" ] && [ "$STRM_VOLUMES" != "/vol00:/vol00:ro" ] && [ "$STRM_VOLUMES" != "/vol00:/vol00:ro|" ]; then
        echo "📦 当前Volume配置:"
        echo "$STRM_VOLUMES" | tr '|' '\n' | sed 's/^/   /'
        echo ""

        COMPOSE_FILE="/data/docker-compose.yml"
        if [ -f "$COMPOSE_FILE" ]; then
            echo "✅ Docker Compose文件已更新（原始位置）: ${COMPOSE_FILE}"
            echo ""
            echo "ℹ️  如路径在容器内暂不可访问，请点击面板'重启容器'按钮，或执行："
            echo "   cd /data"
            echo "   docker compose restart"
            echo ""
        fi
    fi
fi

echo "🚀 正在启动 FNYSFD..."
echo ""

exec /app/fnysfd -c "$CONFIG_FILE" "$@"
