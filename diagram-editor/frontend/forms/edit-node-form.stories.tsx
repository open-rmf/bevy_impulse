import { useArgs } from '@storybook/preview-api';
import type { Meta, StoryObj } from 'storybook-react-rsbuild';

import EditNodeForm from './edit-node-form';

const meta: Meta<typeof EditNodeForm> = {
  component: EditNodeForm,
  title: 'Forms/EditNodeForm',
};

export default meta;

type Story = StoryObj<typeof EditNodeForm>;

export const Node: Story = {
  args: {
    node: {
      id: 'node-1',
      type: 'node',
      position: { x: 0, y: 0 },
      data: {
        opId: 'opId',
        type: 'node',
        builder: 'builder',
        next: '',
      },
    },
  },
  render: function Render(args) {
    const [, updateArgs] = useArgs();
    return (
      <EditNodeForm
        {...args}
        onChange={(change) => {
          updateArgs({ node: change.item });
        }}
      />
    );
  },
};

export const Buffer: Story = {
  args: {
    node: {
      id: 'buffer-1',
      type: 'buffer',
      position: { x: 0, y: 0 },
      data: {
        opId: 'opId',
        type: 'buffer',
        serialize: false,
      },
    },
  },
  render: function Render(args) {
    const [, updateArgs] = useArgs();
    return (
      <EditNodeForm
        {...args}
        onChange={(change) => {
          updateArgs({ node: change.item });
        }}
      />
    );
  },
};
