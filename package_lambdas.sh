#!/usr/bin/env bash
# package_lambdas.sh
#
# Builds a deployment zip for every Lambda under src/lambdas/. Each zip
# bundles the handler module, the shared `common/` package, and any
# pip dependencies listed in that function's local requirements.txt.
#
# Usage:
#   ./package_lambdas.sh                 # build all
#   ./package_lambdas.sh ingest_handler  # build a single function
#
# Output: build/<function>.zip
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
SRC_DIR="$ROOT_DIR/src/lambdas"
BUILD_DIR="$ROOT_DIR/build"
COMMON_DIR="$SRC_DIR/common"

mkdir -p "$BUILD_DIR"

# Functions packaged as standalone Lambda artifacts.
FUNCTIONS=(
  ingest_handler
  transform_handler
  dq_handler
  load_handler
  glue_trigger
  api_ingestion
)

# Optional filter from CLI args.
if [ "$#" -gt 0 ]; then
  FUNCTIONS=("$@")
fi

build_one() {
  local fn="$1"
  local fn_dir="$SRC_DIR/$fn"
  local stage_dir
  stage_dir="$(mktemp -d)"
  trap 'rm -rf "$stage_dir"' RETURN

  if [ ! -d "$fn_dir" ]; then
    echo "skipping $fn (not found)"
    return 0
  fi

  echo "==> packaging $fn"
  cp -R "$fn_dir/." "$stage_dir/"
  cp -R "$COMMON_DIR" "$stage_dir/common"

  if [ -f "$fn_dir/requirements.txt" ]; then
    pip install \
      --quiet \
      --no-compile \
      --target "$stage_dir" \
      -r "$fn_dir/requirements.txt"
  fi

  # Strip caches before zipping to keep the artifact small.
  find "$stage_dir" -name "__pycache__" -type d -prune -exec rm -rf {} +
  find "$stage_dir" -name "*.dist-info" -type d -prune -exec rm -rf {} +

  local out="$BUILD_DIR/$fn.zip"
  rm -f "$out"
  (cd "$stage_dir" && zip -qr "$out" .)
  echo "    -> $out ($(du -h "$out" | cut -f1))"
}

for fn in "${FUNCTIONS[@]}"; do
  build_one "$fn"
done

echo "done."
