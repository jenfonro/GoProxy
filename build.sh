#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

if ! command -v go >/dev/null 2>&1; then
  echo "go not found in PATH" >&2
  exit 1
fi

OUT_DIR="$ROOT_DIR/build"
mkdir -p "$OUT_DIR"

GOOS="$(go env GOOS)"
GOARCH="$(go env GOARCH)"
GOEXE="$(go env GOEXE)"
OUT_PATH="$OUT_DIR/goproxy${GOEXE}"
echo "[build] building for ${GOOS}-${GOARCH}..."

CGO_ENABLED=0 \
  go build -buildvcs=false -trimpath -ldflags "-s -w" \
  -o "$OUT_PATH" .

chmod +x "$OUT_PATH" || true
echo "[build] done: $OUT_PATH"
