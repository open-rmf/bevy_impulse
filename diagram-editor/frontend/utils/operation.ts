import type { DiagramEditorEdge } from '../edges';
import type {
  BufferSelection,
  BuiltinTarget,
  DiagramOperation,
  NextOperation,
} from '../types/diagram';
import { exhaustiveCheck } from './exhaustive-check';

/**
 * Encodes a `NextOperation` into a node id for react flow.
 * Returns `null` if the operation is "dispose".
 */
export function nextOperationToNodeId(next: NextOperation): string | null {
  if (typeof next === 'string') {
    return next;
  }
  if (isBuiltin(next)) {
    if (next.builtin === 'dispose') {
      return null;
    }
    return `builtin:${next.builtin}`;
  }
  const [namespace, opId] = Object.entries(next)[0];
  return `${namespace}:${opId}`;
}

/**
 * Decodes a node id from react flow to a `NextOperation`.
 */
export function nodeIdToNextOperation(nodeId: string): NextOperation {
  if (nodeId.startsWith('builtin:')) {
    return { builtin: nodeId.slice('builtin:'.length) };
  }
  if (nodeId.includes(':')) {
    const parts = nodeId.split(':', 2);
    return { [parts[0]]: parts[1] };
  }
  return nodeId;
}

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

export function buildEdges(
  op: DiagramOperation,
  opId: string,
): DiagramEditorEdge[] {
  switch (op.type) {
    case 'buffer': {
      return [];
    }
    case 'buffer_access':
    case 'join':
    case 'serialized_join':
    case 'listen': {
      const edges: DiagramEditorEdge[] = [];
      if (isArrayBufferSelection(op.buffers)) {
        for (const [idx, buffer] of op.buffers.entries()) {
          const source = nextOperationToNodeId(buffer);
          if (source) {
            edges.push({
              id: `${source}->${opId}-${idx}`,
              type: 'bufferSeq',
              source,
              target: opId,
              data: { seq: idx },
            });
          }
        }
      } else if (isKeyedBufferSelection(op.buffers)) {
        for (const [key, buffer] of Object.entries(op.buffers)) {
          const source = nextOperationToNodeId(buffer);
          if (source) {
            edges.push({
              id: `${source}->${opId}-${key}`,
              type: 'bufferKey',
              source,
              target: opId,
              data: { key },
            });
          }
        }
      } else {
        const source = nextOperationToNodeId(op.buffers);
        if (source) {
          edges.push({
            id: `${source}->${opId}-0`,
            type: 'bufferSeq',
            source,
            target: opId,
            data: { seq: 0 },
          });
        }
      }

      const target = nextOperationToNodeId(op.next);
      if (target) {
        edges.push({
          id: `${opId}->${target}`,
          type: 'default',
          source: opId,
          target,
          data: {},
        });
      }

      return edges;
    }
    case 'node':
    case 'transform': {
      const target = nextOperationToNodeId(op.next);
      return target
        ? [
            {
              id: `${opId}->${target}`,
              type: 'default',
              source: opId,
              target,
              data: {},
            },
          ]
        : [];
    }
    case 'fork_clone': {
      const edges: DiagramEditorEdge[] = [];
      for (const [idx, next] of op.next.entries()) {
        const target = nextOperationToNodeId(next);
        if (target) {
          edges.push({
            id: `${opId}->${target}-${idx}`,
            type: 'default',
            source: opId,
            target,
            data: {},
          });
        }
      }
      return edges;
    }
    case 'unzip': {
      const edges: DiagramEditorEdge[] = [];
      for (const [idx, next] of op.next.entries()) {
        const target = nextOperationToNodeId(next);
        if (target) {
          edges.push({
            id: `${opId}->${target}-${idx}`,
            type: 'unzip',
            source: opId,
            target,
            data: { seq: idx },
          });
        }
      }
      return edges;
    }
    case 'fork_result': {
      const okTarget = nextOperationToNodeId(op.ok);
      const errTarget = nextOperationToNodeId(op.err);
      const edges: DiagramEditorEdge[] = [];
      if (okTarget) {
        edges.push({
          id: `${opId}->${okTarget}-ok`,
          type: 'forkResultOk',
          source: opId,
          target: okTarget,
          data: {},
        });
      }
      if (errTarget) {
        edges.push({
          id: `${opId}->${errTarget}-err`,
          type: 'forkResultErr',
          source: opId,
          target: errTarget,
          data: {},
        });
      }
      return edges;
    }
    case 'split': {
      const edges: DiagramEditorEdge[] = [];
      if (op.keyed) {
        for (const [key, next] of Object.entries(op.keyed)) {
          const target = nextOperationToNodeId(next);
          if (target) {
            edges.push({
              id: `${opId}->${target}-${key}`,
              type: 'splitKey',
              source: opId,
              target,
              data: { key },
            });
          }
        }
      }
      if (op.sequential) {
        for (const [idx, next] of op.sequential.entries()) {
          const target = nextOperationToNodeId(next);
          if (target) {
            edges.push({
              id: `${opId}->${target}-${idx}`,
              type: 'splitSeq',
              source: opId,
              target,
              data: { seq: idx },
            });
          }
        }
      }
      if (op.remaining) {
        const target = nextOperationToNodeId(op.remaining);
        if (target) {
          edges.push({
            id: `${opId}->${target}-remaining`,
            type: 'splitRemaining',
            source: opId,
            target,
            data: {},
          });
        }
      }
      return edges;
    }
    case 'section': {
      // TODO: support section
      // if (op.connect) {
      //   return Object.values(op.connect).map<DiagramEditorEdge>((next) => {
      //     const target = nextOperationToNodeId(next);
      //     return {
      //       id: `${opId}->${target}`,
      //       source: opId,
      //       target,
      //       data: { type: EdgeType.Basic },
      //     };
      //   });
      // }
      return [];
    }
    case 'scope':
    case 'stream_out': {
      throw new Error('Not implemented');
    }
    default: {
      exhaustiveCheck(op);
      throw new Error('unknown op');
    }
  }
}

export function isBuiltin(
  next: NextOperation,
): next is { builtin: BuiltinTarget } {
  return typeof next === 'object' && 'builtin' in next;
}
