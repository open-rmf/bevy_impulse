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
  onAdd?: (change: NodeAddChange<DiagramEditorNode>) => void;
}

function createNodeChange(
  op: DiagramOperation,
): NodeAddChange<DiagramEditorNode> {
  return {
    type: 'add',
    item: {
      id: uuidv4(),
      type: op.type,
      position: { x: 0, y: 0 },
      data: {
        namespace: '',
        opId: uuidv4(),
        op,
      },
      width: LAYOUT_OPTIONS.nodeWidth,
      height: LAYOUT_OPTIONS.nodeHeight,
    },
  };
}

function AddOperation({ onAdd }: AddOperationProps) {
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
            createNodeChange({
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
          onAdd?.(createNodeChange({ type: 'fork_clone', next: [] }))
        }
      >
        Fork Clone
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<UnzipIcon />}
        onClick={() => onAdd?.(createNodeChange({ type: 'unzip', next: [] }))}
      >
        Unzip
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<ForkResultIcon />}
        onClick={() => {
          onAdd?.(
            createNodeChange({
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
        onClick={() => onAdd?.(createNodeChange({ type: 'split' }))}
      >
        Split
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<JoinIcon />}
        onClick={() => {
          onAdd?.(
            createNodeChange({
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
            createNodeChange({
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
            createNodeChange({
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
        onClick={() => onAdd?.(createNodeChange({ type: 'buffer' }))}
      >
        Buffer
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<BufferAccessIcon />}
        onClick={() => {
          onAdd?.(
            createNodeChange({
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
            createNodeChange({
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
