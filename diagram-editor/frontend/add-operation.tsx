import { Button, ButtonGroup, styled } from '@mui/material';
import type { NodeAddChange, XYPosition } from '@xyflow/react';
import React from 'react';
import { v4 as uuidv4 } from 'uuid';
import { EditorMode, useEditorMode } from './editor-mode';
import { useNodeManager } from './node-manager';
import type { DiagramEditorNode } from './nodes';
import {
  BufferAccessIcon,
  BufferIcon,
  createOperationNode,
  createScopeNode,
  createSectionBufferNode,
  createSectionInputNode,
  createSectionOutputNode,
  ForkCloneIcon,
  ForkResultIcon,
  isOperationNode,
  isSectionBufferNode,
  isSectionInputNode,
  isSectionOutputNode,
  JoinIcon,
  ListenIcon,
  NodeIcon,
  ScopeIcon,
  SectionBufferIcon,
  type SectionBufferNode,
  SectionIcon,
  SectionInputIcon,
  type SectionInputNode,
  SectionOutputIcon,
  type SectionOutputNode,
  SplitIcon,
  StreamOutIcon,
  TransformIcon,
  UnzipIcon,
} from './nodes';
import type { DiagramOperation, NextOperation } from './types/api';
import { joinNamespaces, ROOT_NAMESPACE } from './utils/namespace';
import { addUniqueSuffix } from './utils/unique-value';

const StyledOperationButton = styled(Button)({
  justifyContent: 'flex-start',
});

export interface AddOperationProps {
  parentId?: string;
  newNodePosition: XYPosition;
  onAdd?: (change: NodeAddChange<DiagramEditorNode>[]) => void;
}

function createSectionInputChange(
  remappedId: string,
  targetId: NextOperation,
  position: XYPosition,
): NodeAddChange<SectionInputNode> {
  return {
    type: 'add',
    item: createSectionInputNode(remappedId, targetId, position),
  };
}

function createSectionOutputChange(
  outputId: string,
  position: XYPosition,
): NodeAddChange<SectionOutputNode> {
  return {
    type: 'add',
    item: createSectionOutputNode(outputId, position),
  };
}

function createSectionBufferChange(
  remappedId: string,
  targetId: NextOperation,
  position: XYPosition,
): NodeAddChange<SectionBufferNode> {
  return {
    type: 'add',
    item: createSectionBufferNode(remappedId, targetId, position),
  };
}

function createNodeChange(
  namespace: string,
  parentId: string | undefined,
  newNodePosition: XYPosition,
  op: DiagramOperation,
): NodeAddChange<DiagramEditorNode>[] {
  if (op.type === 'scope') {
    return createScopeNode(
      namespace,
      parentId,
      newNodePosition,
      op,
      uuidv4(),
    ).map((node) => ({ type: 'add', item: node }));
  }

  return [
    {
      type: 'add',
      item: createOperationNode(
        namespace,
        parentId,
        newNodePosition,
        op,
        uuidv4(),
      ),
    },
  ];
}

function AddOperation({ parentId, newNodePosition, onAdd }: AddOperationProps) {
  const [editorMode] = useEditorMode();
  const nodeManager = useNodeManager();
  const namespace = React.useMemo(() => {
    const parentNode = parentId && nodeManager.tryGetNode(parentId);
    if (!parentNode || !isOperationNode(parentNode)) {
      return ROOT_NAMESPACE;
    }
    return joinNamespaces(parentNode.data.namespace, parentNode.data.opId);
  }, [parentId, nodeManager]);

  return (
    <ButtonGroup
      orientation="vertical"
      variant="contained"
      size="small"
      aria-label="Add operation button group"
    >
      {editorMode.mode === EditorMode.Template &&
        namespace === ROOT_NAMESPACE && (
          <StyledOperationButton
            startIcon={<SectionInputIcon />}
            onClick={() => {
              const remappedId = addUniqueSuffix(
                'new_input',
                nodeManager.nodes
                  .filter(isSectionInputNode)
                  .map((n) => n.data.remappedId),
              );
              onAdd?.([
                createSectionInputChange(
                  remappedId,
                  { builtin: 'dispose' },
                  newNodePosition,
                ),
              ]);
            }}
          >
            Section Input
          </StyledOperationButton>
        )}
      {editorMode.mode === EditorMode.Template &&
        namespace === ROOT_NAMESPACE && (
          <StyledOperationButton
            startIcon={<SectionOutputIcon />}
            onClick={() => {
              const outputId = addUniqueSuffix(
                'new_output',
                nodeManager.nodes
                  .filter(isSectionOutputNode)
                  .map((n) => n.data.outputId),
              );
              onAdd?.([createSectionOutputChange(outputId, newNodePosition)]);
            }}
          >
            Section Output
          </StyledOperationButton>
        )}
      {editorMode.mode === EditorMode.Template &&
        namespace === ROOT_NAMESPACE && (
          <StyledOperationButton
            startIcon={<SectionBufferIcon />}
            onClick={() => {
              const remappedId = addUniqueSuffix(
                'new_buffer',
                nodeManager.nodes
                  .filter(isSectionBufferNode)
                  .map((n) => n.data.remappedId),
              );
              onAdd?.([
                createSectionBufferChange(
                  remappedId,
                  { builtin: 'dispose' },
                  newNodePosition,
                ),
              ]);
            }}
          >
            Section Buffer
          </StyledOperationButton>
        )}
      <StyledOperationButton
        startIcon={<NodeIcon />}
        onClick={() => {
          onAdd?.(
            createNodeChange(namespace, parentId, newNodePosition, {
              type: 'node',
              builder: '',
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
          onAdd?.(
            createNodeChange(namespace, parentId, newNodePosition, {
              type: 'fork_clone',
              next: [],
            }),
          )
        }
      >
        Fork Clone
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<UnzipIcon />}
        onClick={() =>
          onAdd?.(
            createNodeChange(namespace, parentId, newNodePosition, {
              type: 'unzip',
              next: [],
            }),
          )
        }
      >
        Unzip
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<ForkResultIcon />}
        onClick={() => {
          onAdd?.(
            createNodeChange(namespace, parentId, newNodePosition, {
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
        onClick={() =>
          onAdd?.(
            createNodeChange(namespace, parentId, newNodePosition, {
              type: 'split',
            }),
          )
        }
      >
        Split
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<JoinIcon />}
        onClick={() => {
          onAdd?.(
            createNodeChange(namespace, parentId, newNodePosition, {
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
        startIcon={<TransformIcon />}
        onClick={() => {
          onAdd?.(
            createNodeChange(namespace, parentId, newNodePosition, {
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
        onClick={() =>
          onAdd?.(
            createNodeChange(namespace, parentId, newNodePosition, {
              type: 'buffer',
            }),
          )
        }
      >
        Buffer
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<BufferAccessIcon />}
        onClick={() => {
          onAdd?.(
            createNodeChange(namespace, parentId, newNodePosition, {
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
            createNodeChange(namespace, parentId, newNodePosition, {
              type: 'listen',
              buffers: [],
              next: { builtin: 'dispose' },
            }),
          )
        }
      >
        Listen
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<StreamOutIcon />}
        onClick={() =>
          onAdd?.(
            createNodeChange(namespace, parentId, newNodePosition, {
              type: 'stream_out',
              name: '',
            }),
          )
        }
      >
        Stream Out
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<ScopeIcon />}
        onClick={() =>
          onAdd?.(
            createNodeChange(namespace, parentId, newNodePosition, {
              type: 'scope',
              start: { builtin: 'dispose' },
              ops: {},
              next: { builtin: 'dispose' },
            }),
          )
        }
      >
        Scope
      </StyledOperationButton>
      <StyledOperationButton
        startIcon={<SectionIcon />}
        onClick={() =>
          onAdd?.(
            createNodeChange(namespace, parentId, newNodePosition, {
              type: 'section',
              template: '',
            }),
          )
        }
      >
        Section
      </StyledOperationButton>
    </ButtonGroup>
  );
}

export default AddOperation;
