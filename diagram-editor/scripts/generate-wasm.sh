#!/usr/bin/bash
set -e

TMPDIR=$(mktemp -d)
trap "rm -r '$TMPDIR'" EXIT

wasm-pack build --dev --out-name stub --out-dir "$TMPDIR" ../examples/diagram/calculator_wasm
sed -i '1i // biome-ignore-all lint: generated' "$TMPDIR/stub.d.ts"
cp "$TMPDIR/stub.d.ts" frontend/api-client/wasm/
