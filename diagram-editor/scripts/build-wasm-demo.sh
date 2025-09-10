#!/usr/bin/bash
set -euo pipefail

wasm-pack build ../examples/diagram/calculator_wasm
RSBUILD_OUTPUT_PREFIX=/bevy_impulse_diagram_editor_demo WASM_PKG_PATH=../examples/diagram/calculator_wasm/pkg WASM_PKG_NAME=calculator_wasm rsbuild build --environment wasm-backend
