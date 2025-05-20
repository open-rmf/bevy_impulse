import { defineConfig } from '@rsbuild/core';
import { pluginReact } from '@rsbuild/plugin-react';

export default defineConfig({
  html: {
    meta: {
      viewport: 'width=device-width, initial-scale=1.0',
    },
  },
  plugins: [pluginReact()],
  environments: {
    test: {
      output: {
        target: 'node',
      },
    },
  },
});
