import { defineConfig } from '@rsbuild/core';
import { pluginReact } from '@rsbuild/plugin-react';

export default defineConfig({
  output: {
    assetPrefix: process.env.RSBUILD_OUTPUT_PREFIX || '/',
  },
  server: {
    proxy: {
      '/api': 'http://localhost:3001',
    },
  },
  source: {
    entry: {
      index: './frontend/index.tsx',
    },
  },
  html: {
    title: 'Bevy Impulse Diagram Editor',
    meta: {
      viewport: 'width=device-width, initial-scale=1.0',
    },
  },
  plugins: [pluginReact()],
  environments: {
    'rest-backend': {},
    'wasm-backend': {
      resolve: {
        alias: {
          './rest-client.ts': './frontend/api-client/wasm-client',
          './wasm-stub/stub.js': `${process.env.WASM_PKG_PATH}/${process.env.WASM_PKG_NAME}.js`,
        },
      },
    },
  },
});
