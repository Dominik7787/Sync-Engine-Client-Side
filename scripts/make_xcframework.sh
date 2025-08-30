#!/usr/bin/env bash
set -euo pipefail

CRATE_NAME=sync-engine
LIB_NAME=sync_engine
OUT_DIR=$(pwd)/dist
HDR=${OUT_DIR}/sync_engine.h
MODULE_NAME=SyncEngineCore

rm -rf "${OUT_DIR}"
mkdir -p "${OUT_DIR}"

# Generate C header
if ! command -v cbindgen >/dev/null 2>&1; then
  echo "cbindgen is required. Install: cargo install cbindgen" >&2
  exit 1
fi
cbindgen --crate ${CRATE_NAME} --config cbindgen.toml --output "${HDR}"

# Build for iOS device and simulator
IOS_TARGETS=(aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios)
for TARGET in "${IOS_TARGETS[@]}"; do
  rustup target add "$TARGET" || true
  cargo build --release --target "$TARGET"
done

ROOT=$(pwd)
LIB_A=lib${LIB_NAME}.a
FRAMEWORK_DIR=${OUT_DIR}/SyncEngine.xcframework
rm -rf "$FRAMEWORK_DIR"

function make_slice() {
  local TARGET=$1
  local PLATFORM=$2
  local SLICE_DIR=${OUT_DIR}/${PLATFORM}
  mkdir -p "$SLICE_DIR/Headers"
  cp "${HDR}" "$SLICE_DIR/Headers/"
  # Generate module.modulemap so Swift can import the C header as a module
  cat > "$SLICE_DIR/Headers/module.modulemap" <<'MMAP'
module MODULE_NAME_REPLACED {
  header "sync_engine.h"
  export *
}
MMAP
  sed -i '' "s/MODULE_NAME_REPLACED/${MODULE_NAME}/g" "$SLICE_DIR/Headers/module.modulemap"
  cp "${ROOT}/target/${TARGET}/release/${LIB_A}" "$SLICE_DIR/"
}

make_slice aarch64-apple-ios ios-arm64
make_slice aarch64-apple-ios-sim ios-arm64-simulator
make_slice x86_64-apple-ios ios-x86_64-simulator

# Create XCFramework
rm -rf "$FRAMEWORK_DIR"
xcodebuild -create-xcframework \
  -library ${OUT_DIR}/ios-arm64/${LIB_A} -headers ${OUT_DIR}/ios-arm64/Headers \
  -library ${OUT_DIR}/ios-arm64-simulator/${LIB_A} -headers ${OUT_DIR}/ios-arm64-simulator/Headers \
  -library ${OUT_DIR}/ios-x86_64-simulator/${LIB_A} -headers ${OUT_DIR}/ios-x86_64-simulator/Headers \
  -output "$FRAMEWORK_DIR"

echo "Built: ${FRAMEWORK_DIR}"

