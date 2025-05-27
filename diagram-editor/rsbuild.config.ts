import { defineConfig } from '@rsbuild/core';
import { pluginReact } from '@rsbuild/plugin-react';

export default defineConfig({
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
  output: {
    assetPrefix: '/diagram_editor',
  },
  dev: {
    assetPrefix: '/diagram_editor',
  },
  plugins: [pluginReact()],
});
