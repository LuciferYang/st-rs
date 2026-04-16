#!/usr/bin/env bash
# Build a fat JAR containing the native library for the current platform.
#
# Usage:
#   ./build-fat-jar.sh           # debug build
#   ./build-fat-jar.sh --release # release build
#
# Output: java/target/st-rs-jni-0.0.1-SNAPSHOT.jar
#
# The JAR contains the native library at:
#   org/forstdb/native/<os>-<arch>/libst_rs_jni.{so,dylib}

set -euo pipefail
cd "$(dirname "$0")"

PROFILE="debug"
CARGO_FLAGS=""
if [[ "${1:-}" == "--release" ]]; then
    PROFILE="release"
    CARGO_FLAGS="--release"
fi

# 1. Build the native library.
echo "==> Building native library (${PROFILE})..."
cargo build -p st-rs-jni ${CARGO_FLAGS}

# 2. Detect platform.
OS="$(uname -s)"
ARCH="$(uname -m)"

case "${OS}" in
    Linux)  OS_NAME="linux" ;;
    Darwin) OS_NAME="osx"   ;;
    *)      OS_NAME="${OS}"  ;;
esac

case "${ARCH}" in
    x86_64|amd64)   ARCH_NAME="x86_64"  ;;
    aarch64|arm64)   ARCH_NAME="aarch64" ;;
    *)               ARCH_NAME="${ARCH}"  ;;
esac

PLATFORM="${OS_NAME}-${ARCH_NAME}"

case "${OS}" in
    Linux)  LIB_NAME="libst_rs_jni.so"    ;;
    Darwin) LIB_NAME="libst_rs_jni.dylib"  ;;
    *)      LIB_NAME="libst_rs_jni.so"    ;;
esac

SRC="target/${PROFILE}/${LIB_NAME}"
if [[ ! -f "${SRC}" ]]; then
    echo "ERROR: ${SRC} not found. Build failed?"
    exit 1
fi

# 3. Copy native library into JAR resources.
RESOURCE_DIR="java/src/main/resources/org/forstdb/native/${PLATFORM}"
mkdir -p "${RESOURCE_DIR}"
cp "${SRC}" "${RESOURCE_DIR}/${LIB_NAME}"
echo "==> Copied ${SRC} → ${RESOURCE_DIR}/${LIB_NAME}"

# 4. Build the JAR.
echo "==> Building JAR..."
cd java
mvn package -DskipTests -q
echo "==> Done: java/target/st-rs-jni-0.0.1-SNAPSHOT.jar"
echo "    Native library bundled for: ${PLATFORM}"
