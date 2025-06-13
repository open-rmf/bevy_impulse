import { useArgs } from '@storybook/preview-api';
import type { Meta, StoryObj } from 'storybook-react-rsbuild';

import type {
  BufferKeyEdge,
  BufferSeqEdge,
  ForkResultErrEdge,
  ForkResultOkEdge,
  SplitKeyEdge,
  SplitRemainingEdge,
  SplitSeqEdge,
  UnzipEdge,
} from '../edges';
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
      onChange={(change) => {
        updateArgs({ edge: change.item });
      }}
    />
  );
};

export const BufferKey: Story = {
  args: {
    edge: {
      id: 'edge-1',
      source: 'a',
      target: 'b',
      type: 'bufferKey',
      data: {
        key: 'testKey',
      },
    } as BufferKeyEdge,
  },
  render,
};

export const BufferSeq: Story = {
  args: {
    edge: {
      id: 'edge-1',
      source: 'a',
      target: 'b',
      type: 'bufferSeq',
      data: {
        seq: 1,
      },
    } as BufferSeqEdge,
  },
  render,
};

export const ForkResultOk: Story = {
  args: {
    edge: {
      id: 'edge-1',
      source: 'a',
      target: 'b',
      type: 'forkResultOk',
    } as ForkResultOkEdge,
  },
  render,
};

export const ForkResultErr: Story = {
  args: {
    edge: {
      id: 'edge-1',
      source: 'a',
      target: 'b',
      type: 'forkResultErr',
    } as ForkResultErrEdge,
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
    } as SplitKeyEdge,
  },
  render,
};

export const SplitSeq: Story = {
  args: {
    edge: {
      id: 'edge-1',
      source: 'a',
      target: 'b',
      type: 'splitSeq',
      data: {
        seq: 2,
      },
    } as SplitSeqEdge,
  },
  render,
};

export const SplitRemaining: Story = {
  args: {
    edge: {
      id: 'edge-1',
      source: 'a',
      target: 'b',
      type: 'splitRemaining',
      data: {},
    } as SplitRemainingEdge,
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
    } as UnzipEdge,
  },
  render,
};
