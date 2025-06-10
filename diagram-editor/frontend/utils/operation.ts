import { EdgeType, type DiagramEditorEdge } from '../nodes';
import type {
  BufferSelection,
  DiagramOperation,
  NamespacedOperation,
  NextOperation,
} from '../types/diagram';

/**
 * Encodes a `NextOperation` into a node id for react flow.
 */
export function nextOperationToNodeId(next: NextOperation): string {
  if (typeof next === 'string') {
    return next;
  }
  if ('builtin' in next) {
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

/**
 * Encodes a `BufferSelection` into an array of node ids.
 */
export function bufferSelectionToNodeIds(buffer: BufferSelection): string[] {
  if (typeof buffer === 'string') {
    return [nextOperationToNodeId(buffer)];
  }
  if (Array.isArray(buffer)) {
    return buffer.map((b) => nextOperationToNodeId(b));
  }
  return [nextOperationToNodeId(buffer as NamespacedOperation)];
}

/**
 * Decodes an array of node ids into a `BufferSelection`
 */
export function nodeIdsToBufferSelection(nodeIds: string[]): BufferSelection {
  if (nodeIds.length === 0) {
    return [];
  }
  if (nodeIds[0].includes(':')) {
    const bufferSelection: Record<string, NextOperation> = {};
    for (const nodeId of nodeIds) {
      const parts = nodeId.split(':', 2);
      bufferSelection[parts[0]] = parts[1];
    }
    return bufferSelection;
  }
  return nodeIds.map((nodeId) => nodeIdToNextOperation(nodeId));
}

export function buildEdges(
  op: DiagramOperation,
  opId: string,
): DiagramEditorEdge[] {
  switch (op.type) {
    case 'buffer': {
      return [];
      // for (const joinNode of allNodes) {
      //   if (
      //     joinNode.data.type === 'join' ||
      //     joinNode.data.type === 'serialized_join'
      //   ) {
      //     const buffers = bufferSelectionToNodeIds(joinNode.data.buffers);
      //     if (buffers.includes(node.id)) {
      //       nextIds.push(joinNode.id);
      //     }
      //   }
      // }
    }
    case 'buffer_access':
    case 'join':
    case 'serialized_join':
    case 'listen': {
      const edges: DiagramEditorEdge[] = [];
      if (isArrayBufferSelection(op.buffers)) {
        edges.push(
          ...op.buffers.map<DiagramEditorEdge>((buffer, idx) => {
            const source = nextOperationToNodeId(buffer);
            return {
              id: `${source}->${opId}-${idx}`,
              source,
              target: opId,
              data: { type: EdgeType.BufferSeq, seq: idx },
            };
          }),
        );
      } else if (isKeyedBufferSelection(op.buffers)) {
        edges.push(
          ...Object.entries(op.buffers).map<DiagramEditorEdge>(
            ([key, buffer]) => {
              const source = nextOperationToNodeId(buffer);
              return {
                id: `${source}->${opId}-${key}`,
                source,
                target: opId,
                data: { type: EdgeType.BufferKey, key },
              };
            },
          ),
        );
      } else {
        const source = nextOperationToNodeId(op.buffers);
        edges.push({
          id: `${source}->${opId}-0`,
          source,
          target: opId,
          data: { type: EdgeType.BufferSeq, seq: 0 },
        });
      }

      const target = nextOperationToNodeId(op.next);
      edges.push({
        id: `${opId}->${target}`,
        source: opId,
        target,
        data: { type: EdgeType.Basic },
      });

      return edges;
    }
    case 'node':
    case 'transform': {
      const target = nextOperationToNodeId(op.next);
      return [
        {
          id: `${opId}->${target}`,
          source: opId,
          target,
          data: { type: EdgeType.Basic },
        },
      ];
    }
    case 'fork_clone': {
      return op.next.map((next, idx) => {
        const target = nextOperationToNodeId(next);
        return {
          id: `${opId}->${target}-${idx}`,
          source: opId,
          target,
          data: { type: EdgeType.Basic },
        };
      });
    }
    case 'unzip': {
      return op.next.map((next, idx) => {
        const target = nextOperationToNodeId(next);
        return {
          id: `${opId}->${target}-${idx}`,
          source: opId,
          target,
          data: { type: EdgeType.Unzip, seq: idx },
        };
      });
    }
    case 'fork_result': {
      const okTarget = nextOperationToNodeId(op.ok);
      const errTarget = nextOperationToNodeId(op.err);
      return [
        {
          id: `${opId}->${okTarget}-ok`,
          source: opId,
          target: okTarget,
          data: { type: EdgeType.ForkResultOk },
        },
        {
          id: `${opId}->${okTarget}-err`,
          source: opId,
          target: errTarget,
          data: { type: EdgeType.ForkResultErr },
        },
      ];
    }
    case 'split': {
      const edges: DiagramEditorEdge[] = [];
      if (op.keyed) {
        edges.push(
          ...Object.entries(op.keyed).map<DiagramEditorEdge>(([key, next]) => {
            const target = nextOperationToNodeId(next);
            return {
              id: `${opId}->${target}-${key}`,
              source: opId,
              target,
              data: { type: EdgeType.SplitKey, key },
            };
          }),
        );
      }
      if (op.sequential) {
        edges.push(
          ...op.sequential.map<DiagramEditorEdge>((next, idx) => {
            const target = nextOperationToNodeId(next);
            return {
              id: `${opId}->${target}-${idx}`,
              source: opId,
              target,
              data: { type: EdgeType.SplitSequential, seq: idx },
            };
          }),
        );
      }
      if (op.remaining) {
        const target = nextOperationToNodeId(op.remaining);
        edges.push({
          id: `${opId}->${target}-remaining`,
          source: opId,
          target,
          data: { type: EdgeType.SplitRemaining },
        });
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
