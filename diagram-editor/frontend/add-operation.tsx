import { Button, ButtonGroup, styled } from '@mui/material';
import {
  useReactFlow,
  type NodeAddChange,
  type XYPosition,
} from '@xyflow/react';
import React from 'react';
import { v4 as uuidv4 } from 'uuid';
import { EditorMode, useEditorMode } from './editor-mode';
import type {
  DiagramEditorNode,
  SectionInterfaceNode,
  SectionInterfaceNodeTypes,
} from './nodes';
import {
  BufferAccessIcon,
  BufferIcon,
  ForkCloneIcon,
  ForkResultIcon,
  isOperationNode,
  JoinIcon,
  ListenIcon,
  NodeIcon,
  ScopeIcon,
  SectionBufferIcon,
  SectionInputIcon,
  SectionOutputIcon,
  SerializedJoinIcon,
  SplitIcon,
  TransformIcon,
  UnzipIcon,
} from './nodes';
import type { DiagramOperation } from './types/api';
import { joinNamespaces, ROOT_NAMESPACE } from './utils';
import { calculateScopeBounds, LAYOUT_OPTIONS } from './utils/layout';

const StyledOperationButton = styled(Button)({
  justifyContent: 'flex-start',
});

export interface AddOperationProps {
  parentId?: string;
  newNodePosition: XYPosition;
  onAdd?: (change: NodeAddChange<DiagramEditorNode>[]) => void;
}

function createSectionInterfaceChange(
  type: SectionInterfaceNodeTypes,
  connectKey: string,
  position: XYPosition,
): NodeAddChange<SectionInterfaceNode> {
  return {
    type: 'add',
    item: {
      id: uuidv4(),
      type,
      data: { namespace: ROOT_NAMESPACE, connectKey },
      position,
    },
  };
}

function createNodeChange(
  namespace: string,
  parentId: string | undefined,
  newNodePosition: XYPosition,
  op: DiagramOperation,
): NodeAddChange<DiagramEditorNode>[] {
  if (op.type === 'scope') {
    const scopeId = uuidv4();
    const children: NodeAddChange<DiagramEditorNode>[] = [
      {
        type: 'add',
        item: {
          id: uuidv4(),
          type: 'start',
          position: {
            x: LAYOUT_OPTIONS.scopePadding.leftRight,
            y: LAYOUT_OPTIONS.scopePadding.topBottom,
          },
          data: {
            namespace: joinNamespaces(namespace, scopeId),
          },
          width: LAYOUT_OPTIONS.nodeWidth,
          height: LAYOUT_OPTIONS.nodeHeight,
          parentId: scopeId,
        },
      },
      {
        type: 'add',
        item: {
          id: uuidv4(),
          type: 'terminate',
          position: {
            x: LAYOUT_OPTIONS.scopePadding.leftRight,
            y: LAYOUT_OPTIONS.scopePadding.topBottom * 5,
          },
          data: {
            namespace: joinNamespaces(namespace, scopeId),
          },
          width: LAYOUT_OPTIONS.nodeWidth,
          height: LAYOUT_OPTIONS.nodeHeight,
          parentId: scopeId,
        },
      },
    ];
    const scopeBounds = calculateScopeBounds(
      children.map((c) => c.item.position),
    );
    return [
      {
        type: 'add',
        item: {
          id: scopeId,
          type: 'scope',
          position: {
            x: newNodePosition.x + scopeBounds.x,
            y: newNodePosition.y + scopeBounds.y,
          },
          data: {
            namespace,
            opId: uuidv4(),
            op,
          },
          width: scopeBounds.width,
          height: scopeBounds.height,
          zIndex: -1,
          parentId,
        },
      },
      ...children,
    ];
  }

  return [
    {
      type: 'add',
      item: {
        id: uuidv4(),
        type: op.type,
        position: newNodePosition,
        data: {
          namespace,
          opId: uuidv4(),
          op,
        },
        width: LAYOUT_OPTIONS.nodeWidth,
        height: LAYOUT_OPTIONS.nodeHeight,
        parentId,
      },
    },
  ];
}

function AddOperation({ parentId, newNodePosition, onAdd }: AddOperationProps) {
  const [editorMode] = useEditorMode();
  const reactFlow = useReactFlow<DiagramEditorNode>();
  const namespace = React.useMemo(() => {
    const parentNode = parentId && reactFlow.getNode(parentId);
    if (!parentNode || !isOperationNode(parentNode)) {
      return ROOT_NAMESPACE;
    }
    return joinNamespaces(parentNode.data.namespace, parentNode.data.opId);
  }, [parentId, reactFlow.getNode]);

  return (
    <ButtonGroup
      orientation="vertical"
      variant="contained"
      size="small"
      aria-label="Add operation button group"
    >
      {editorMode.mode === EditorMode.Template &&
        namespace === ROOT_NAMESPACE && (
          <>
            <StyledOperationButton
              startIcon={<SectionInputIcon />}
              onClick={() => {
                onAdd?.([
                  createSectionInterfaceChange(
                    'sectionInput',
                    'input',
                    newNodePosition,
                  ),
                ]);
              }}
            >
              Section Input
            </StyledOperationButton>
            <StyledOperationButton
              startIcon={<SectionOutputIcon />}
              onClick={() => {
                onAdd?.([
                  createSectionInterfaceChange(
                    'sectionOutput',
                    'output',
                    newNodePosition,
                  ),
                ]);
              }}
            >
              Section Output
            </StyledOperationButton>
            <StyledOperationButton
              startIcon={<SectionBufferIcon />}
              onClick={() => {
                onAdd?.([
                  createSectionInterfaceChange(
                    'sectionBuffer',
                    'buffer',
                    newNodePosition,
                  ),
                ]);
              }}
            >
              Section Buffer
            </StyledOperationButton>
          </>
        )}
      <StyledOperationButton
        startIcon={<NodeIcon />}
        onClick={() => {
          onAdd?.(
            createNodeChange(namespace, parentId, newNodePosition, {
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
        startIcon={<SerializedJoinIcon />}
        onClick={() =>
          onAdd?.(
            createNodeChange(namespace, parentId, newNodePosition, {
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
    </ButtonGroup>
  );
}

export default AddOperation;
