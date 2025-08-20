import { defineConfig } from '@rsbuild/core';
import { pluginReact } from '@rsbuild/plugin-react';

export default defineConfig({
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
});
