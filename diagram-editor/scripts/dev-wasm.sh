#!/usr/bin/bash
set -euo pipefail

SKIP_WASM_PACK=false

if [ $# -gt 0 ]; then
    OPTS=$(getopt -o '' --long skip-wasm-pack -- "$@")
    if [ $? != 0 ]; then echo "Failed to parse options." >&2; exit 1; fi
    eval set -- "$OPTS"

    while true; do
      case "$1" in
        --skip-wasm-pack) SKIP_WASM_PACK=true; shift ;;
        --) shift; break ;;
        *) echo "Invalid args!"; exit 1 ;;
      esac
    done
fi


if [ "$SKIP_WASM_PACK" == false ]; then
  wasm-pack build --dev ../examples/diagram/calculator_wasm
fi

WASM_PKG_PATH=../examples/diagram/calculator_wasm/pkg WASM_PKG_NAME=calculator_wasm pnpm rsbuild dev --environment wasm-backend
