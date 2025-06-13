import { useArgs } from '@storybook/preview-api';
import type { Meta, StoryObj } from 'storybook-react-rsbuild';

import TransformForm from './transform-form';

const meta: Meta<typeof TransformForm> = {
  component: TransformForm,
  title: 'Forms/TransformForm',
};

export default meta;

type Story = StoryObj<typeof TransformForm>;

const render: Story['render'] = (args) => {
  const [, updateArgs] = useArgs();
  return (
    <TransformForm
      {...args}
      onChange={(change) => {
        updateArgs({ node: change.item });
      }}
    />
  );
};

export const Default: Story = {
  args: {
    node: {
      id: 'transform-1',
      type: 'transform',
      position: { x: 0, y: 0 },
      data: {
        opId: 'transform-op-1',
        type: 'transform',
        cel: 'input.value + 1',
        next: '',
      },
    },
  },
  render,
};
