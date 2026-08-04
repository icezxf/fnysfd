#!/bin/bash
# FNYSFD v3.3.11 Docker 镜像构建脚本
# 使用方法:
#   1. 将 fnysfd-v3.3.1 目录上传到 NAS 或服务器
#   2. cd fnysfd-v3.3.1
#   3. chmod +x build-docker.sh
#   4. ./build-docker.sh
#   5. docker images | grep fnysfd  (确认镜像已构建)

set -e

echo "╔══════════════════════════════════════╗"
echo "║  FNYSFD v3.3.11 Docker 镜像构建     ║"
echo "╚══════════════════════════════════════╝"
echo ""

# 构建镜像
echo "📦 正在构建 Docker 镜像 fnysfd:3.3.11 ..."
docker build -t fnysfd:3.3.11 .

echo ""
echo "✅ 镜像构建完成!"
echo ""
echo "📋 镜像信息:"
docker images fnysfd:3.3.11
echo ""

# 导出为 tar（可选）
read -p "是否导出为 tar 文件? (y/N): " export_tar
if [ "$export_tar" = "y" ] || [ "$export_tar" = "Y" ]; then
    echo "📦 正在导出 fnysfd-3.3.11.tar ..."
    docker save -o fnysfd-3.3.11.tar fnysfd:3.3.11
    echo "✅ 已导出: fnysfd-3.3.11.tar"
    ls -lh fnysfd-3.3.11.tar
fi

echo ""
echo "🚀 部署方法:"
echo "   1. docker-compose up -d"
echo "   2. 访问面板: http://你的IP:28006 (admin/admin)"
echo ""
