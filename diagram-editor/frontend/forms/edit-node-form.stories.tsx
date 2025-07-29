import { useArgs } from '@storybook/preview-api';
import type { Meta, StoryObj } from 'storybook-react-rsbuild';
import { ROOT_NAMESPACE } from '../utils/namespace';
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
        namespace: ROOT_NAMESPACE,
        opId: 'testOpId',
        op: {
          type: 'node',
          builder: 'builder',
          next: { builtin: 'dispose' },
        },
      },
    },
  },
  render: function Render(args) {
    const [, updateArgs] = useArgs();
    return (
      <EditNodeForm
        {...args}
        onChanges={(changes) => {
          if (changes.length > 0 && changes[0].type === 'replace') {
            updateArgs({ node: changes[0].item });
          }
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
        namespace: ROOT_NAMESPACE,
        opId: 'testOpId',
        op: {
          type: 'buffer',
          serialize: false,
        },
      },
    },
  },
  render: function Render(args) {
    const [, updateArgs] = useArgs();
    return (
      <EditNodeForm
        {...args}
        onChanges={(changes) => {
          if (changes.length > 0 && changes[0].type === 'replace') {
            updateArgs({ node: changes[0].item });
          }
        }}
      />
    );
  },
};

export const Transform: Story = {
  args: {
    node: {
      id: 'transform-1',
      type: 'transform',
      position: { x: 0, y: 0 },
      data: {
        namespace: ROOT_NAMESPACE,
        opId: 'testOpId',
        op: {
          type: 'transform',
          cel: '',
          next: { builtin: 'dispose' },
        },
      },
    },
  },
  render: function Render(args) {
    const [, updateArgs] = useArgs();
    return (
      <EditNodeForm
        {...args}
        onChanges={(changes) => {
          if (changes.length > 0 && changes[0].type === 'replace') {
            updateArgs({ node: changes[0].item });
          }
        }}
      />
    );
  },
};
