#!/usr/bin/bash
set -euo pipefail

wasm-pack build --dev ../examples/diagram/calculator_wasm
WASM_PKG_PATH=../examples/diagram/calculator_wasm/pkg WASM_PKG_NAME=calculator_wasm pnpm build:wasm
