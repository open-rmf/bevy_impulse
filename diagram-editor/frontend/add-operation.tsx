import { Button, ButtonGroup, styled } from '@mui/material';
import type { NodeAddChange } from '@xyflow/react';
import { v4 as uuidv4 } from 'uuid';
import {
  BufferAccessIcon,
  BufferIcon,
  ForkCloneIcon,
  ForkResultIcon,
  JoinIcon,
  ListenIcon,
  NodeIcon,
  SerializedJoinIcon,
  SplitIcon,
  TransformIcon,
  UnzipIcon,
} from './nodes/icons';
import type { DiagramEditorNode, DiagramOperation } from './types';
import { LAYOUT_OPTIONS } from './utils/layout';

const StyledOperationButton = styled(Button)({
  justifyContent: 'flex-start',
});

export interface AddOperationProps {
  namespace: string;
  onAdd?: (change: NodeAddChange<DiagramEditorNode>) => void;
}

function createNodeChange(
  namespace: string,
  op: DiagramOperation,
): NodeAddChange<DiagramEditorNode> {
  return {
    type: 'add',
    item: {
      id: uuidv4(),
      type: op.type,
      position: { x: 0, y: 0 },
      data: {
        namespace,
        opId: uuidv4(),
        op,
      },
      width: LAYOUT_OPTIONS.nodeWidth,
      height: LAYOUT_OPTIONS.nodeHeight,
      zIndex: op.type === 'scope' ? -1 : undefined,
    },
  };
}

function AddOperation({ namespace, onAdd }: AddOperationProps) {
  return (
    <ButtonGroup
      orientation="vertical"
      variant="contained"
      size="small"
      aria-label="Add operation button group"
    >
      <StyledOperationButton
        startIcon={<NodeIcon />}
        onClick={() => {
          onAdd?.(
            createNodeChange(namespace, {
              type: 'node',
              builder: 'new_node',
              next: { builtin: 'dispose' },
            }),
          );
        }}
      >
        Node
      </StyledOperationButton>
      {/* <Button>Section</Button> */}
      <StyledOperationButton
        startIcon={<ForkCloneIcon />}
        onClick={() =>
          onAdd?.(createNodeChange(namespace, { type: 'fork_clone', next: [] }))
        }
      >
        Fork Clone
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<UnzipIcon />}
        onClick={() =>
          onAdd?.(createNodeChange(namespace, { type: 'unzip', next: [] }))
        }
      >
        Unzip
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<ForkResultIcon />}
        onClick={() => {
          onAdd?.(
            createNodeChange(namespace, {
              type: 'fork_result',
              err: { builtin: 'dispose' },
              ok: { builtin: 'dispose' },
            }),
          );
        }}
      >
        Fork Result
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<SplitIcon />}
        onClick={() => onAdd?.(createNodeChange(namespace, { type: 'split' }))}
      >
        Split
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<JoinIcon />}
        onClick={() => {
          onAdd?.(
            createNodeChange(namespace, {
              type: 'join',
              buffers: [],
              next: { builtin: 'dispose' },
            }),
          );
        }}
      >
        Join
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<SerializedJoinIcon />}
        onClick={() =>
          onAdd?.(
            createNodeChange(namespace, {
              type: 'serialized_join',
              buffers: [],
              next: { builtin: 'dispose' },
            }),
          )
        }
      >
        Serialized Join
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<TransformIcon />}
        onClick={() => {
          onAdd?.(
            createNodeChange(namespace, {
              type: 'transform',
              cel: '',
              next: { builtin: 'dispose' },
            }),
          );
        }}
      >
        Transform
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<BufferIcon />}
        onClick={() => onAdd?.(createNodeChange(namespace, { type: 'buffer' }))}
      >
        Buffer
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<BufferAccessIcon />}
        onClick={() => {
          onAdd?.(
            createNodeChange(namespace, {
              type: 'buffer_access',
              buffers: [],
              next: { builtin: 'dispose' },
            }),
          );
        }}
      >
        Buffer Access
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<ListenIcon />}
        onClick={() =>
          onAdd?.(
            createNodeChange(namespace, {
              type: 'listen',
              buffers: [],
              next: { builtin: 'dispose' },
            }),
          )
        }
      >
        Listen
      </StyledOperationButton>
    </ButtonGroup>
  );
}

export default AddOperation;
