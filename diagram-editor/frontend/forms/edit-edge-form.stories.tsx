import { useArgs } from '@storybook/preview-api';
import type { Meta, StoryObj } from 'storybook-react-rsbuild';

import EditEdgeForm from './edit-edge-form';

const meta: Meta<typeof EditEdgeForm> = {
  component: EditEdgeForm,
  title: 'Forms/EditEdgeForm',
};

export default meta;

type Story = StoryObj<typeof EditEdgeForm>;

const render: Story['render'] = (args) => {
  const [, updateArgs] = useArgs();
  return (
    <EditEdgeForm
      {...args}
      onChanges={(change) => {
        updateArgs({ edge: change.item });
      }}
    />
  );
};

export const BufferEdge: Story = {
  args: {
    edge: {
      id: 'edge-1',
      source: 'a',
      target: 'b',
      type: 'bufferKey',
      data: {
        key: 'testKey',
      },
    },
    allowedEdgeTypes: ['bufferKey', 'bufferSeq'],
  },
  render,
};

export const ForkResult: Story = {
  args: {
    edge: {
      id: 'edge-1',
      source: 'a',
      target: 'b',
      type: 'forkResultOk',
      data: {},
    },
    allowedEdgeTypes: ['forkResultOk', 'forkResultErr'],
  },
  render,
};

export const SplitKey: Story = {
  args: {
    edge: {
      id: 'edge-1',
      source: 'a',
      target: 'b',
      type: 'splitKey',
      data: {
        key: 'splitTestKey',
      },
    },
    allowedEdgeTypes: ['splitKey', 'splitSeq', 'splitRemaining'],
  },
  render,
};

export const Unzip: Story = {
  args: {
    edge: {
      id: 'edge-1',
      source: 'a',
      target: 'b',
      type: 'unzip',
      data: {
        seq: 3,
      },
    },
    allowedEdgeTypes: ['unzip'],
  },
  render,
};

export const Default: Story = {
  args: {
    edge: {
      id: 'edge-1',
      source: 'a',
      target: 'b',
      type: 'default',
      data: {},
    },
    allowedEdgeTypes: ['default'],
  },
  render,
};
