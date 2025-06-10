import { EdgeType, type DiagramEditorEdge } from '../nodes';
import type {
  BufferSelection,
  BuiltinTarget,
  DiagramOperation,
  NextOperation,
} from '../types/diagram';

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
              source,
              target: opId,
              data: { type: EdgeType.BufferSeq, seq: idx },
            });
          }
        }
      } else if (isKeyedBufferSelection(op.buffers)) {
        for (const [key, buffer] of Object.entries(op.buffers)) {
          const source = nextOperationToNodeId(buffer);
          if (source) {
            edges.push({
              id: `${source}->${opId}-${key}`,
              source,
              target: opId,
              data: { type: EdgeType.BufferKey, key },
            });
          }
        }
      } else {
        const source = nextOperationToNodeId(op.buffers);
        if (source) {
          edges.push({
            id: `${source}->${opId}-0`,
            source,
            target: opId,
            data: { type: EdgeType.BufferSeq, seq: 0 },
          });
        }
      }

      const target = nextOperationToNodeId(op.next);
      if (target) {
        edges.push({
          id: `${opId}->${target}`,
          source: opId,
          target,
          data: { type: EdgeType.Basic },
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
              source: opId,
              target,
              data: { type: EdgeType.Basic },
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
            source: opId,
            target,
            data: { type: EdgeType.Basic },
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
            source: opId,
            target,
            data: { type: EdgeType.Unzip, seq: idx },
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
          source: opId,
          target: okTarget,
          data: { type: EdgeType.ForkResultOk },
        });
      }
      if (errTarget) {
        edges.push({
          id: `${opId}->${errTarget}-err`,
          source: opId,
          target: errTarget,
          data: { type: EdgeType.ForkResultErr },
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
              source: opId,
              target,
              data: { type: EdgeType.SplitKey, key },
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
              source: opId,
              target,
              data: { type: EdgeType.SplitSequential, seq: idx },
            });
          }
        }
      }
      if (op.remaining) {
        const target = nextOperationToNodeId(op.remaining);
        if (target) {
          edges.push({
            id: `${opId}->${target}-remaining`,
            source: opId,
            target,
            data: { type: EdgeType.SplitRemaining },
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
    default: {
      return [];
    }
  }
}

export function isBuiltin(
  next: NextOperation,
): next is { builtin: BuiltinTarget } {
  return typeof next === 'object' && 'builtin' in next;
}
