#!/usr/bin/env bash
set -euo pipefail

ROOT=$(pwd)
OUT_DIR=${ROOT}/dist

bash ${ROOT}/scripts/make_xcframework.sh

cd "${OUT_DIR}"
ZIP_NAME=SyncEngine.xcframework.zip
rm -f "$ZIP_NAME"
zip -r "$ZIP_NAME" SyncEngine.xcframework > /dev/null

if ! command -v swift >/dev/null 2>&1; then
  echo "swift toolchain is required to compute checksum" >&2
  exit 1
fi
CSUM=$(swift package compute-checksum "$ZIP_NAME")
echo "Checksum: ${CSUM}"
echo "Zip: ${OUT_DIR}/${ZIP_NAME}"
echo "Replace Package.swift checksum and URL to your GitHub Release asset."


