import { Button, ButtonGroup, styled } from '@mui/material';
import { v4 as uuidv4 } from 'uuid';
import type { NodeAddChange } from '@xyflow/react';
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
import type { DiagramEditorNode } from './types';

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
              id: uuidv4(),
              type: 'node',
              position: { x: 0, y: 0 },
              data: {
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
              id: uuidv4(),
              type: 'fork_clone',
              position: { x: 0, y: 0 },
              data: {
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
              id: uuidv4(),
              type: 'unzip',
              position: { x: 0, y: 0 },
              data: {
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
        startIcon={<ForkResultIcon />}
        onClick={() => {
          onAdd?.({
            item: {
              id: uuidv4(),
              type: 'fork_result',
              position: { x: 0, y: 0 },
              data: {
                type: 'fork_result',
                err: { builtin: 'dispose' },
                ok: { builtin: 'dispose' },
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
              id: uuidv4(),
              type: 'split',
              position: { x: 0, y: 0 },
              data: {
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
              id: uuidv4(),
              type: 'join',
              position: { x: 0, y: 0 },
              data: {
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
              id: uuidv4(),
              type: 'serialized_join',
              position: { x: 0, y: 0 },
              data: {
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
              id: uuidv4(),
              type: 'transform',
              position: { x: 0, y: 0 },
              data: {
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
              id: uuidv4(),
              type: 'buffer',
              position: { x: 0, y: 0 },
              data: {
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
              id: uuidv4(),
              type: 'buffer_access',
              position: { x: 0, y: 0 },
              data: {
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
              id: uuidv4(),
              type: 'listen',
              position: { x: 0, y: 0 },
              data: {
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
