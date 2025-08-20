import type { StorybookConfig } from 'storybook-react-rsbuild';

const config: StorybookConfig = {
  stories: [
    '../frontend/**/*.mdx',
    '../frontend/**/*.stories.@(js|jsx|mjs|ts|tsx)',
  ],
  addons: [],
  framework: 'storybook-react-rsbuild',
};
export default config;
