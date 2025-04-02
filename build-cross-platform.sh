#!/bin/bash
set -e

echo "===== 开始跨平台编译 ====="

# 创建输出目录
mkdir -p dist

# 1. 编译 macOS ARM64 版本 (当前平台)
echo "正在编译 macOS ARM64 (Apple Silicon) 版本..."
cargo build --release
cp target/release/transmuta dist/transmuta-macos-arm64

# 2. 编译 macOS x86_64 版本
echo "正在编译 macOS x86_64 (Intel) 版本..."
cargo build --release --target x86_64-apple-darwin
cp target/x86_64-apple-darwin/release/transmuta dist/transmuta-macos-x86_64

# 3. 使用 Docker 编译 Linux 版本
echo "正在为 Linux 版本构建 Docker 镜像..."
docker build -t transmuta-build -f Dockerfile.cross .

# 创建临时容器并复制编译好的二进制文件
echo "从 Docker 容器中提取 Linux 二进制文件..."
container_id=$(docker create transmuta-build)
docker cp $container_id:/usr/src/app/target/release/transmuta dist/transmuta-linux-x86_64
docker rm $container_id

# 使二进制文件可执行
chmod +x dist/transmuta-macos-arm64
chmod +x dist/transmuta-macos-x86_64
chmod +x dist/transmuta-linux-x86_64

echo "===== 跨平台编译完成 ====="
echo "二进制文件位于 dist/ 目录:"
ls -la dist/

echo
echo "注意: 要在 Linux 上执行这些二进制文件，可能需要安装必要的库依赖。" 