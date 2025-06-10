import { Button, ButtonGroup, styled } from '@mui/material';
import type { NodeAddChange } from '@xyflow/react';
import type { DiagramEditorNode } from './nodes';
import {
  BufferAccessIcon,
  BufferIcon,
  ForkCloneIcon,
  ForkResult,
  JoinIcon,
  ListenIcon,
  NodeIcon,
  SerializedJoinIcon,
  SplitIcon,
  TransformIcon,
  UnzipIcon,
} from './nodes/icons';

const StyledOperationButton = styled(Button)({
  justifyContent: 'flex-start',
});

export interface AddOperationProps {
  onAdd?: (change: NodeAddChange<DiagramEditorNode>) => void;
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
          onAdd?.({
            item: {
              id: 'new_operation',
              type: 'inputOutput',
              position: { x: 0, y: 0 },
              data: {
                opId: 'new_operation',
                type: 'node',
                builder: '',
                next: { builtin: 'dispose' },
              },
            },
            type: 'add',
          });
        }}
      >
        Node
      </StyledOperationButton>
      {/* <Button>Section</Button> */}
      <StyledOperationButton
        startIcon={<ForkCloneIcon />}
        onClick={() => {
          onAdd?.({
            item: {
              id: 'new_operation',
              type: 'inputOutput',
              position: { x: 0, y: 0 },
              data: {
                opId: 'new_operation',
                type: 'fork_clone',
                next: [],
              },
            },
            type: 'add',
          });
        }}
      >
        Fork Clone
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<UnzipIcon />}
        onClick={() => {
          onAdd?.({
            item: {
              id: 'new_operation',
              type: 'inputOutput',
              position: { x: 0, y: 0 },
              data: {
                opId: 'new_operation',
                type: 'unzip',
                next: [],
              },
            },
            type: 'add',
          });
        }}
      >
        Unzip
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<ForkResult />}
        onClick={() => {
          onAdd?.({
            item: {
              id: 'new_operation',
              type: 'inputOutput',
              position: { x: 0, y: 0 },
              data: {
                opId: 'new_operation',
                type: 'fork_result',
                err: '',
                ok: '',
              },
            },
            type: 'add',
          });
        }}
      >
        Fork Result
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<SplitIcon />}
        onClick={() => {
          onAdd?.({
            item: {
              id: 'new_operation',
              type: 'inputOutput',
              position: { x: 0, y: 0 },
              data: {
                opId: 'new_operation',
                type: 'split',
              },
            },
            type: 'add',
          });
        }}
      >
        Split
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<JoinIcon />}
        onClick={() => {
          onAdd?.({
            item: {
              id: 'new_operation',
              type: 'inputOutput',
              position: { x: 0, y: 0 },
              data: {
                opId: 'new_operation',
                type: 'join',
                buffers: [],
                next: { builtin: 'dispose' },
              },
            },
            type: 'add',
          });
        }}
      >
        Join
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<SerializedJoinIcon />}
        onClick={() => {
          onAdd?.({
            item: {
              id: 'new_operation',
              type: 'inputOutput',
              position: { x: 0, y: 0 },
              data: {
                opId: 'new_operation',
                type: 'serialized_join',
                buffers: [],
                next: { builtin: 'dispose' },
              },
            },
            type: 'add',
          });
        }}
      >
        Serialized Join
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<TransformIcon />}
        onClick={() => {
          onAdd?.({
            item: {
              id: 'new_operation',
              type: 'inputOutput',
              position: { x: 0, y: 0 },
              data: {
                opId: 'new_operation',
                type: 'transform',
                cel: '',
                next: { builtin: 'dispose' },
              },
            },
            type: 'add',
          });
        }}
      >
        Transform
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<BufferIcon />}
        onClick={() => {
          onAdd?.({
            item: {
              id: 'new_operation',
              type: 'inputOutput',
              position: { x: 0, y: 0 },
              data: {
                opId: 'new_operation',
                type: 'buffer',
              },
            },
            type: 'add',
          });
        }}
      >
        Buffer
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<BufferAccessIcon />}
        onClick={() => {
          onAdd?.({
            item: {
              id: 'new_operation',
              type: 'inputOutput',
              position: { x: 0, y: 0 },
              data: {
                opId: 'new_operation',
                type: 'buffer_access',
                buffers: [],
                next: { builtin: 'dispose' },
              },
            },
            type: 'add',
          });
        }}
      >
        Buffer Access
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<ListenIcon />}
        onClick={() => {
          onAdd?.({
            item: {
              id: 'new_operation',
              type: 'inputOutput',
              position: { x: 0, y: 0 },
              data: {
                opId: 'new_operation',
                type: 'listen',
                buffers: [],
                next: { builtin: 'dispose' },
              },
            },
            type: 'add',
          });
        }}
      >
        Listen
      </StyledOperationButton>
    </ButtonGroup>
  );
}

export default AddOperation;
