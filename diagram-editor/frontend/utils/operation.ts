import {
  BufferFetchType,
  createBufferEdge,
  createDefaultEdge,
  createForkResultErrEdge,
  createForkResultOkEdge,
  createSectionEdge,
  createSplitKeyEdge,
  createSplitRemainingEdge,
  createSplitSeqEdge,
  createStreamOutEdge,
  createUnzipEdge,
  type DiagramEditorEdge,
} from '../edges';
import { HandleId } from '../handles';
import { NodeManager } from '../node-manager';
import {
  type DiagramEditorNode,
  isOperationNode,
  type OperationNode,
  START_ID,
} from '../nodes';
import type {
  BufferSelection,
  BuiltinTarget,
  DiagramOperation,
  NextOperation,
} from '../types/api';
import { exhaustiveCheck } from './exhaustive-check';
import { joinNamespaces, ROOT_NAMESPACE } from './namespace';

export function isKeyedBufferSelection(
  bufferSelection: BufferSelection,
): bufferSelection is Record<string, NextOperation> {
  return typeof bufferSelection !== 'string' && !Array.isArray(bufferSelection);
}

export function isArrayBufferSelection(
  bufferSelection: BufferSelection,
): bufferSelection is NextOperation[] {
  return Array.isArray(bufferSelection);
}

export function withTargetInput<T extends DiagramEditorEdge>(
  edge: T,
  next: NextOperation,
): T {
  if (typeof next === 'object' && !isBuiltin(next)) {
    edge.data.input = {
      type: 'sectionInput',
      inputId: Object.values(next)[0],
    };
  }
  return edge;
}

function createStreamOutEdges(
  streamOuts: Record<string, NextOperation>,
  node: OperationNode,
  nodeManager: NodeManager,
): DiagramEditorEdge[] {
  const edges: DiagramEditorEdge[] = [];
  for (const [streamId, nextOp] of Object.entries(streamOuts)) {
    const targetNode = nodeManager.getNodeFromNextOp(
      node.data.namespace,
      nextOp,
    );
    if (targetNode) {
      const target = targetNode.id;
      edges.push(
        withTargetInput(
          createStreamOutEdge(node.id, HandleId.DataStream, target, null, {
            streamId,
          }),
          nextOp,
        ),
      );
    }
  }
  return edges;
}

function getBufferFetchType(
  node: OperationNode,
  key: string | number,
): BufferFetchType | null {
  if (node.type !== 'join' || node.data.op.type !== 'join') {
    return null;
  }
  if (node.data.op.clone?.includes(key)) {
    return BufferFetchType.Clone;
  } else {
    return BufferFetchType.Pull;
  }
}

function createBufferEdges(
  node: OperationNode,
  buffers: BufferSelection,
  nodeManager: NodeManager,
): DiagramEditorEdge[] {
  const edges: DiagramEditorEdge[] = [];
  if (isArrayBufferSelection(buffers)) {
    for (const [idx, buffer] of buffers.entries()) {
      const source = nodeManager.getNodeFromNextOp(
        node.data.namespace,
        buffer,
      )?.id;
      if (source) {
        const fetchType = getBufferFetchType(node, idx);
        const edge = createBufferEdge(source, null, node.id, null, {
          type: 'bufferSeq',
          seq: idx,
          ...(fetchType !== null ? { fetchType } : {}),
        });
        if (typeof buffer === 'object' && !isBuiltin(buffer)) {
          edge.data.output = { bufferId: Object.values(buffer)[0] };
        }
        edges.push(edge);
      }
    }
  } else if (isKeyedBufferSelection(buffers)) {
    for (const [key, buffer] of Object.entries(buffers)) {
      const source = nodeManager.getNodeFromNextOp(
        node.data.namespace,
        buffer,
      )?.id;
      if (source) {
        const fetchType = getBufferFetchType(node, key);
        const edge = createBufferEdge(source, null, node.id, null, {
          type: 'bufferKey',
          key,
          ...(fetchType !== null ? { fetchType } : {}),
        });
        if (typeof buffer === 'object' && !isBuiltin(buffer)) {
          edge.data.output = { bufferId: Object.values(buffer)[0] };
        }
        edges.push(edge);
      }
    }
  }

  return edges;
}

export function buildEdges(nodes: DiagramEditorNode[]): DiagramEditorEdge[] {
  const edges: DiagramEditorEdge[] = [];
  const nodeManager = new NodeManager(nodes);
  const addDataEdge = (edge: DiagramEditorEdge, next: NextOperation) => {
    edges.push(withTargetInput(edge, next));
  };

  interface State {
    namespace: string;
    opId: string;
    op: DiagramOperation;
  }
  const stack = [
    ...nodes.map(
      (node) =>
        ({
          namespace: ROOT_NAMESPACE,
          opId: node.data.opId,
          op: node.data.op,
        }) as State,
    ),
  ];

  for (const node of nodes) {
    if (isOperationNode(node)) {
      const op = node.data.op as DiagramOperation;
      const opId = node.data.opId;

      switch (op.type) {
        case 'buffer': {
          break;
        }
        case 'buffer_access':
        case 'join':
        case 'listen': {
          edges.push(...createBufferEdges(node, op.buffers, nodeManager));

          const nextNodeId = nodeManager.getNodeFromNextOp(
            node.data.namespace,
            op.next,
          )?.id;
          if (nextNodeId) {
            addDataEdge(
              createDefaultEdge(node.id, null, nextNodeId, null),
              op.next,
            );
          }

          break;
        }
        case 'node': {
          const target = nodeManager.getNodeFromNextOp(
            node.data.namespace,
            op.next,
          )?.id;
          if (target) {
            addDataEdge(
              createDefaultEdge(node.id, null, target, null),
              op.next,
            );
          }
          if (op.stream_out) {
            edges.push(
              ...createStreamOutEdges(op.stream_out, node, nodeManager),
            );
          }
          break;
        }
        case 'script': {
          const target = nodeManager.getNodeFromNextOp(
            node.data.namespace,
            op.next,
          )?.id;
          if (target) {
            addDataEdge(
              createDefaultEdge(node.id, null, target, null),
              op.next,
            );
          }
          if (op.stream_out) {
            edges.push(
              ...createStreamOutEdges(op.stream_out, node, nodeManager),
            );
          }
          break;
        }
        case 'transform': {
          const target = nodeManager.getNodeFromNextOp(
            node.data.namespace,
            op.next,
          )?.id;
          if (target) {
            addDataEdge(
              createDefaultEdge(node.id, null, target, null),
              op.next,
            );
          }
          break;
        }
        case 'fork_clone': {
          for (const next of op.next.values()) {
            const target = nodeManager.getNodeFromNextOp(
              node.data.namespace,
              next,
            )?.id;
            if (target) {
              addDataEdge(createDefaultEdge(node.id, null, target, null), next);
            }
          }
          break;
        }
        case 'unzip': {
          for (const [idx, next] of op.next.entries()) {
            const target = nodeManager.getNodeFromNextOp(
              node.data.namespace,
              next,
            )?.id;
            if (target) {
              addDataEdge(
                createUnzipEdge(node.id, null, target, null, { seq: idx }),
                next,
              );
            }
          }
          break;
        }
        case 'fork_result': {
          const okTarget = nodeManager.getNodeFromNextOp(
            node.data.namespace,
            op.ok,
          )?.id;
          const errTarget = nodeManager.getNodeFromNextOp(
            node.data.namespace,
            op.err,
          )?.id;
          if (okTarget) {
            addDataEdge(
              createForkResultOkEdge(
                node.id,
                HandleId.ForkResultOk,
                okTarget,
                null,
              ),
              op.ok,
            );
          }
          if (errTarget) {
            addDataEdge(
              createForkResultErrEdge(
                node.id,
                HandleId.ForkResultErr,
                errTarget,
                null,
              ),
              op.err,
            );
          }
          break;
        }
        case 'split': {
          if (op.keyed) {
            for (const [key, next] of Object.entries(op.keyed)) {
              const target = nodeManager.getNodeFromNextOp(
                node.data.namespace,
                next,
              )?.id;
              if (target) {
                addDataEdge(
                  createSplitKeyEdge(node.id, null, target, null, { key }),
                  next,
                );
              }
            }
          }
          if (op.sequential) {
            for (const [idx, next] of op.sequential.entries()) {
              const target = nodeManager.getNodeFromNextOp(
                node.data.namespace,
                next,
              )?.id;
              if (target) {
                addDataEdge(
                  createSplitSeqEdge(node.id, null, target, null, { seq: idx }),
                  next,
                );
              }
            }
          }
          if (op.remaining) {
            const target = nodeManager.getNodeFromNextOp(
              node.data.namespace,
              op.remaining,
            )?.id;
            if (target) {
              addDataEdge(
                createSplitRemainingEdge(node.id, null, target, null),
                op.remaining,
              );
            }
          }
          break;
        }
        case 'section': {
          if (op.connect) {
            for (const [outputId, next] of Object.entries(op.connect)) {
              const target = nodeManager.getNodeFromNextOp(
                node.data.namespace,
                next,
              )?.id;
              if (target) {
                addDataEdge(
                  createSectionEdge(node.id, null, target, null, {
                    output: outputId,
                  }),
                  next,
                );
              }
            }
          }
          break;
        }
        case 'scope': {
          const target = nodeManager.getNodeFromNextOp(
            node.data.namespace,
            op.next,
          )?.id;
          if (target) {
            addDataEdge(
              createDefaultEdge(node.id, null, target, null),
              op.next,
            );
          }

          if (op.stream_out) {
            edges.push(
              ...createStreamOutEdges(op.stream_out, node, nodeManager),
            );
          }

          const scopeStart = nodeManager.getNodeFromNamespaceOpId(
            joinNamespaces(node.data.namespace, opId),
            START_ID,
          );
          const scopeStartTarget = nodeManager.getNodeFromNextOp(
            joinNamespaces(node.data.namespace, opId),
            op.start,
          );
          if (scopeStart && scopeStartTarget) {
            addDataEdge(
              createDefaultEdge(scopeStart.id, null, scopeStartTarget.id, null),
              op.start,
            );
          }

          for (const [innerOpId, innerOp] of Object.entries(op.ops)) {
            stack.push({
              namespace: joinNamespaces(node.data.namespace, opId),
              opId: innerOpId,
              op: innerOp,
            });
          }

          break;
        }
        case 'stream_out': {
          break;
        }
        default: {
          exhaustiveCheck(op);
          throw new Error('unknown op');
        }
      }
    } else if (node.type === 'sectionInput' || node.type === 'sectionBuffer') {
      const target = nodeManager.getNodeFromNextOp(
        ROOT_NAMESPACE,
        node.data.targetId,
      )?.id;
      if (target) {
        const edge = withTargetInput(
          createDefaultEdge(node.id, null, target, null),
          node.data.targetId,
        );
        if (
          node.type === 'sectionBuffer' &&
          edge.data.input.type === 'sectionInput'
        ) {
          edge.data.input = { ...edge.data.input, type: 'sectionBuffer' };
        }
        edges.push(edge);
      }
    }
  }

  return edges;
}

export function isBuiltin(next: unknown): next is { builtin: BuiltinTarget } {
  return next !== null && typeof next === 'object' && 'builtin' in next;
}

export function isSectionBuilder(
  nodeData: Extract<DiagramOperation, { type: 'section' }>,
): nodeData is Extract<DiagramOperation, { type: 'section' }> & {
  builder: string;
} {
  return 'builder' in nodeData;
}

export function formatNextOperation(nextOp: NextOperation): string {
  if (isBuiltin(nextOp)) {
    return `builtin:${nextOp.builtin}`;
  }
  if (typeof nextOp === 'object') {
    const [ns, opId] = Object.entries(nextOp)[0];
    return `${ns}:${opId}`;
  }
  return nextOp;
}
