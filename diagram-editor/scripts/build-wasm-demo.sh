#!/usr/bin/bash
set -euo pipefail

: ${REPO_NAME:=crossflow}
wasm-pack build ../examples/diagram/calculator_wasm
RSBUILD_OUTPUT_PREFIX="/${REPO_NAME}" WASM_PKG_PATH=../examples/diagram/calculator_wasm/pkg WASM_PKG_NAME=calculator_wasm rsbuild build --environment wasm-backend
