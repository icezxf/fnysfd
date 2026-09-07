#!/bin/bash
# FNYSFD Docker 镜像构建脚本（支持目标架构，兼容 ARM / x86）
# 使用方法:
#   1. 将 fnysfd 源码目录上传到编译机（NAS 或任意 x86/ARM 机器）
#   2. cd fnysfd
#   3. chmod +x build-docker.sh
#   4. 构建本机架构:          ./build-docker.sh
#      指定目标架构（跨架构编译，例如在 x86 上为 ARM NAS 编译）:
#        ./build-docker.sh arm64     # 飞牛/ARM NAS（aarch64）最常用
#        ./build-docker.sh amd64     # 普通 x86_64 机器
#   5. 导出 tar: 脚本会按参数询问，或加 --tar 参数直接导出
#   6. docker images | grep fnysfd  (确认镜像已构建)

set -e

VERSION="3.3.11"
IMAGE_TAG="fnysfd:${VERSION}"

# ---------- 解析目标架构参数 ----------
# 默认取本机架构；可通过第 1 个参数显式指定目标架构
ARCH="$1"

# 映射架构名 -> Docker 平台
resolve_platform() {
  case "$1" in
    ""|"native")
      # native：按本机架构决定
      local m; m=$(uname -m)
      case "$m" in
        aarch64|arm64|armv8*) echo "linux/arm64" ;;
        x86_64|amd64)         echo "linux/amd64" ;;
        armv7l|armv7|armhf)   echo "linux/arm/v7" ;;
        *) echo "unsupported" ;;
      esac ;;
    arm64|aarch64|armv8)  echo "linux/arm64" ;;
    amd64|x86_64|x86-64)  echo "linux/amd64" ;;
    armv7|arm7|armhf)     echo "linux/arm/v7" ;;
    *) echo "unsupported" ;;
  esac
}

TARGET="$(resolve_platform "$ARCH")"
if [ "$TARGET" = "unsupported" ]; then
  echo "❌ 无法识别的目标架构: '$ARCH'（支持: amd64 / arm64 / armv7 / 留空取本机）"
  exit 1
fi

# 本机原生架构
HOST_PLATFORM="$(resolve_platform native)"

echo "╔══════════════════════════════════════════════════╗"
echo "║  FNYSFD v$VERSION Docker 镜像构建        ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""
echo "  目标架构: ${TARGET}"
echo "  编译机   : ${HOST_PLATFORM}"
echo "  镜像标签 : ${IMAGE_TAG}"
echo ""

# ---------- 构建 ----------
BUILD_PASS=0
if [ "$TARGET" = "$HOST_PLATFORM" ]; then
  echo "📦 本机架构直编 $TARGET ..."
  if docker build -t "$IMAGE_TAG" .; then BUILD_PASS=1; fi
else
  echo "🧩 跨架构编译 $TARGET（编译机为 $HOST_PLATFORM）..."
  # 注：docker build 会设置 BuildKit 的 TARGETARCH 参数，Dockerfile 据此编译对应 Go 架构
  if docker run --rm --privileged multiarch/qemu-user-static --reset -p yes >/dev/null 2>&1; then
    echo "   ✅ 已注册 qemu-user（支持在 $HOST_PLATFORM 上模拟 $TARGET 编译）"
  else
    echo "   ⚠️  未注册 qemu-user-static，若失败请先安装：docker run --rm --privileged multiarch/qemu-user-static --reset -p yes"
  fi
  if docker build --platform "$TARGET" --load -t "$IMAGE_TAG" .; then BUILD_PASS=1; fi
fi

if [ "$BUILD_PASS" != "1" ]; then
  echo "❌ 镜像构建失败"
  exit 1
fi

echo ""
echo "✅ 镜像构建完成!"
echo ""
echo "📋 镜像信息:"
docker images "$IMAGE_TAG"
echo ""

# ---------- 导出为 tar（ARM 部署用） ----------
if [ "$2" = "--tar" ] || [ "$2" = "tar" ]; then
  EXPORT_TAR="y"
else
  read -p "是否导出为 tar 文件? (y/N): " EXPORT_TAR
fi
if [ "$EXPORT_TAR" = "y" ] || [ "$EXPORT_TAR" = "Y" ]; then
  ARCH_SHORT="$(echo "$TARGET" | sed 's|linux/||; s|/|_|g')"
  TAR_NAME="fnysfd-${VERSION}-${ARCH_SHORT}.tar"
  echo "📦 正在导出 $TAR_NAME ..."
  docker save -o "$TAR_NAME" "$IMAGE_TAG"
  echo "✅ 已导出: $TAR_NAME"
  echo ""
  echo "   ⚠️  在 ARM NAS 部署时，请导入并确认架构为 arm64："
  echo "       docker load -i $TAR_NAME"
  echo "       docker image inspect $IMAGE_TAG | grep -i architecture"
  echo ""
fi

echo ""
echo "🚀 部署方法:"
echo "   1. docker load -i <上一步导出的 tar>   （ARM NAS 务必确认是 arm64）"
echo "   2. docker-compose up -d"
echo "   3. 访问面板: http://你的IP:28006 (admin/admin)"
echo ""