#!/usr/bin/bash
set -euo pipefail

scripts/build-wasm-demo.sh
gh-pages -d dist
