#!/bin/bash
set -e

# Disable AWS CLI pager
export AWS_PAGER=""

S3_BUCKET="${S3_BUCKET:-esb-benchmark-results}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$DIR/.."

ARCH="${1:-x86_64}"
STORE="${2}"

if [ -z "$STORE" ]; then
    echo "[ERROR] Store feature must be specified! Usage: ./build-and-upload.sh <ARCH> <STORE>"
    exit 1
fi

# Get Git commit hash
GIT_HASH=$(git -C "$REPO_ROOT" rev-parse --short HEAD)
if ! git -C "$REPO_ROOT" diff --quiet HEAD 2>/dev/null; then
    GIT_HASH="${GIT_HASH}-dirty"
    echo "[WARN] Uncommitted changes detected! Labeling binary as $GIT_HASH"
fi

S3_PREFIX="s3://$S3_BUCKET/binaries/$GIT_HASH"
S3_BINARY_URI="$S3_PREFIX/es-bench-$STORE-$ARCH"
S3_SHA_URI="$S3_PREFIX/es-bench-$STORE-$ARCH.sha256"

echo "================================================="
echo " Checking S3 artifact for commit: $GIT_HASH"
echo " Target Arch: $ARCH | Feature (Store): $STORE"
echo " S3 Path:     $S3_BINARY_URI"
echo "================================================="

# Check if binary already exists in S3
if aws s3 ls "$S3_BINARY_URI" > /dev/null 2>&1; then
    echo " -> [CACHE HIT] Binary for $STORE ($ARCH) already exists in S3. Skipping build."
    exit 0
fi

echo " -> [CACHE MISS] Building binary for feature '$STORE' via Docker..."

# Determine Docker platform flag based on target architecture
DOCKER_PLATFORM="linux/amd64"
if [ "$ARCH" == "aarch64" ]; then
    DOCKER_PLATFORM="linux/arm64"
fi

BUILD_DIR="/tmp/esb-build-$GIT_HASH-$ARCH-$STORE"
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

# Build inside Amazon Linux 2023 Docker container with explicit feature flags
docker run --rm \
    --platform "$DOCKER_PLATFORM" \
    -v "$REPO_ROOT":/code \
    -v "$BUILD_DIR":/out \
    amazonlinux:2023 \
    bash -c "
        set -e
        dnf install -y make protobuf-compiler gcc cargo git protobuf-devel
        cd /code
        CARGO_TARGET_DIR=/tmp/target cargo build -p es-bench --release --features \"$STORE\"
        cp /tmp/target/release/es-bench /out/es-bench
    "

LOCAL_BINARY="$BUILD_DIR/es-bench"

if [ ! -f "$LOCAL_BINARY" ]; then
    echo "[ERROR] Cargo build failed to produce $LOCAL_BINARY"
    exit 1
fi

# Generate SHA256 sum
cd "$BUILD_DIR"
sha256sum es-bench | awk '{print $1}' > es-bench.sha256
SHA_VAL=$(cat es-bench.sha256)

echo "Built binary SHA256 ($STORE): $SHA_VAL"

# Upload binary and checksum to S3
echo "Uploading to S3..."
aws s3 cp "$LOCAL_BINARY" "$S3_BINARY_URI"
aws s3 cp "$BUILD_DIR/es-bench.sha256" "$S3_SHA_URI"

rm -rf "$BUILD_DIR"
echo "Upload complete for $STORE ($ARCH)!"