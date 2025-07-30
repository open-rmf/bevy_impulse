import { v4 as uuidv4 } from 'uuid';
import type { DiagramEditorEdge } from '../edges';
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

function createStreamOutEdges(
  streamOuts: Record<string, NextOperation>,
  node: OperationNode,
  nodeManager: NodeManager,
): DiagramEditorEdge[] {
  const edges: DiagramEditorEdge[] = [];
  for (const nextOp of Object.values(streamOuts)) {
    const target = nodeManager.getNodeFromNextOp(
      node.data.namespace,
      nextOp,
    )?.id;
    if (target) {
      edges.push({
        id: uuidv4(),
        type: 'default',
        source: node.id,
        target,
        data: {},
      });
    }
  }
  return edges;
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
        edges.push({
          id: uuidv4(),
          type: 'bufferSeq',
          source,
          target: node.id,
          data: { seq: idx },
        });
      }
    }
  } else if (isKeyedBufferSelection(buffers)) {
    for (const [key, buffer] of Object.entries(buffers)) {
      const source = nodeManager.getNodeFromNextOp(
        node.data.namespace,
        buffer,
      )?.id;
      if (source) {
        edges.push({
          id: uuidv4(),
          type: 'bufferKey',
          source,
          target: node.id,
          data: { key },
        });
      }
    }
  } else {
    const source = nodeManager.getNodeFromNextOp(
      node.data.namespace,
      buffers,
    )?.id;
    if (source) {
      edges.push({
        id: uuidv4(),
        type: 'bufferSeq',
        source,
        target: node.id,
        data: { seq: 0 },
      });
    }
  }

  return edges;
}

export function buildEdges(nodes: DiagramEditorNode[]): DiagramEditorEdge[] {
  const edges: DiagramEditorEdge[] = [];
  const nodeManager = new NodeManager(nodes);

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
      const op = node.data.op;
      const opId = node.data.opId;

      switch (op.type) {
        case 'buffer': {
          break;
        }
        case 'buffer_access':
        case 'join':
        case 'serialized_join':
        case 'listen': {
          edges.push(...createBufferEdges(node, op.buffers, nodeManager));

          const nextNodeId = nodeManager.getNodeFromNextOp(
            node.data.namespace,
            op.next,
          )?.id;
          if (nextNodeId) {
            edges.push({
              id: uuidv4(),
              type: 'default',
              source: node.id,
              target: nextNodeId,
              data: {},
            });
          }

          break;
        }
        case 'node': {
          const target = nodeManager.getNodeFromNextOp(
            node.data.namespace,
            op.next,
          )?.id;
          if (target) {
            edges.push({
              id: uuidv4(),
              type: 'default',
              source: node.id,
              target,
              data: {},
            });
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
            edges.push({
              id: uuidv4(),
              type: 'default',
              source: node.id,
              target,
              data: {},
            });
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
              edges.push({
                id: uuidv4(),
                type: 'default',
                source: node.id,
                target,
                data: {},
              });
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
              edges.push({
                id: uuidv4(),
                type: 'unzip',
                source: node.id,
                target,
                data: { seq: idx },
              });
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
            edges.push({
              id: uuidv4(),
              type: 'forkResultOk',
              source: node.id,
              target: okTarget,
              data: {},
            });
          }
          if (errTarget) {
            edges.push({
              id: uuidv4(),
              type: 'forkResultErr',
              source: node.id,
              target: errTarget,
              data: {},
            });
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
                edges.push({
                  id: uuidv4(),
                  type: 'splitKey',
                  source: node.id,
                  target,
                  data: { key },
                });
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
                edges.push({
                  id: uuidv4(),
                  type: 'splitSeq',
                  source: node.id,
                  target,
                  data: { seq: idx },
                });
              }
            }
          }
          if (op.remaining) {
            const target = nodeManager.getNodeFromNextOp(
              node.data.namespace,
              op.remaining,
            )?.id;
            if (target) {
              edges.push({
                id: uuidv4(),
                type: 'splitRemaining',
                source: node.id,
                target,
                data: {},
              });
            }
          }
          break;
        }
        case 'section': {
          if (op.connect) {
            for (const next of Object.values(op.connect)) {
              const target = nodeManager.getNodeFromNextOp(
                node.data.namespace,
                next,
              )?.id;
              if (target) {
                edges.push({
                  id: uuidv4(),
                  type: 'default',
                  source: node.id,
                  target,
                  data: {},
                });
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
            edges.push({
              id: uuidv4(),
              type: 'default',
              source: node.id,
              target,
              data: {},
            });
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
            edges.push({
              id: uuidv4(),
              type: 'default',
              source: scopeStart.id,
              target: scopeStartTarget.id,
              data: {},
            });
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
        edges.push({
          id: uuidv4(),
          type: 'default',
          source: node.id,
          target,
          data: {},
        });
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
