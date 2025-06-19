import { START_ID, TERMINATE_ID } from '../nodes';
import type {
  BufferSelection,
  Diagram,
  DiagramEditorEdge,
  DiagramEditorNode,
  DiagramOperation,
  EdgeTypes,
  NodeTypes,
  StreamOutEdge,
} from '../types';
import { exhaustiveCheck } from './exhaustive-check';
import {
  isArrayBufferSelection,
  isKeyedBufferSelection,
  nextOperationToNodeId,
  nodeIdToNextOperation,
} from './operation';

function syncStreamOut(
  sourceOp: Extract<DiagramOperation, { type: 'node' | 'scope' }>,
  edge: StreamOutEdge,
) {
  sourceOp.stream_out = sourceOp.stream_out ? sourceOp.stream_out : {};
  sourceOp.stream_out[edge.data.name] = nodeIdToNextOperation(edge.target);
}

/**
 * Adds a connection to a node data.
 * Nothing will be changed if the edge is invalid. But an error will be thrown if the source node is invalid.
 * This is idempotent, if the connection exists, it will not do anything.
 */
export function syncEdge(diagram: Diagram, edge: DiagramEditorEdge): void {
  if (edge.source === START_ID) {
    diagram.start = edge.target;
    return;
  }
  if (edge.source === TERMINATE_ID) {
    throw new Error('source node cannot be "terminate"');
  }

  if (edge.type === 'bufferKey' || edge.type === 'bufferSeq') {
    syncBufferSelection(diagram, edge);
    return;
  }

  const sourceOp = diagram.ops[edge.source];
  if (!sourceOp) {
    throw new Error(`source operation "${edge.source}" not found`);
  }

  switch (sourceOp.type) {
    case 'node': {
      if (edge.type === 'streamOut') {
        syncStreamOut(sourceOp, edge);
      } else if (edge.type === 'default') {
        sourceOp.next = nodeIdToNextOperation(edge.target);
      }
      break;
    }
    case 'join':
    case 'serialized_join':
    case 'transform':
    case 'buffer_access':
    case 'listen': {
      if (edge.type !== 'default') {
        throw new Error('expected "default" edge');
      }

      sourceOp.next = nodeIdToNextOperation(edge.target);
      break;
    }
    case 'section': {
      throw new Error('TODO');
    }
    case 'fork_clone': {
      if (edge.type !== 'default') {
        throw new Error('expected "default" edge');
      }

      const target = nodeIdToNextOperation(edge.target);
      if (
        !sourceOp.next.some(
          (next) => nextOperationToNodeId(next) === edge.target,
        )
      ) {
        sourceOp.next.push(target);
      }
      break;
    }
    case 'unzip': {
      if (edge.type !== 'unzip') {
        throw new Error('expected "unzip" edge');
      }
      sourceOp.next[edge.data.seq] = nodeIdToNextOperation(edge.target);
      break;
    }
    case 'fork_result': {
      switch (edge.type) {
        case 'forkResultOk': {
          sourceOp.ok = nodeIdToNextOperation(edge.target);
          break;
        }
        case 'forkResultErr': {
          sourceOp.err = nodeIdToNextOperation(edge.target);
          break;
        }
        default: {
          throw new Error('fork_result operation must have "ok" or "err" edge');
        }
      }
      break;
    }
    case 'split': {
      switch (edge.type) {
        case 'splitKey': {
          if (!sourceOp.keyed) {
            sourceOp.keyed = {};
          }
          sourceOp.keyed[edge.data.key] = nodeIdToNextOperation(edge.target);
          break;
        }
        case 'splitSeq': {
          if (!sourceOp.sequential) {
            sourceOp.sequential = [];
          }
          // this works because js allows non-sequential arrays
          sourceOp.sequential[edge.data.seq] = nodeIdToNextOperation(
            edge.target,
          );
          break;
        }
        case 'splitRemaining': {
          sourceOp.remaining = nodeIdToNextOperation(edge.target);
          break;
        }
        default: {
          throw new Error(
            'split operation must have "SplitKey", "SplitSequential", or "SplitRemaining" edge',
          );
        }
      }
      break;
    }
    case 'buffer': {
      throw new Error('buffer operations cannot have connections');
    }
    case 'scope': {
      if (edge.type === 'streamOut') {
        syncStreamOut(sourceOp, edge);
      } else if (edge.type !== 'default') {
        throw new Error('scope operation must have default or streamOut edge');
      }
      sourceOp.next = nodeIdToNextOperation(edge.target);
      break;
    }
    case 'stream_out': {
      break;
    }
    default: {
      exhaustiveCheck(sourceOp);
    }
  }
}

function syncBufferSelection(diagram: Diagram, edge: DiagramEditorEdge) {
  if (edge.type === 'bufferKey' || edge.type === 'bufferSeq') {
    const targetOp = diagram.ops[edge.target];
    if (!targetOp) {
      throw new Error(`target operation "${edge.target}" not found`);
    }
    const bufferSelection = getBufferSelection(targetOp);

    if (
      edge.type === 'bufferKey' &&
      Array.isArray(bufferSelection) &&
      bufferSelection.length === 0
    ) {
      // the array is empty so it is safe to change it to a keyed buffer selection
      setBufferSelection(targetOp, {});
    } else if (
      edge.type === 'bufferSeq' &&
      typeof bufferSelection === 'object' &&
      Object.keys(bufferSelection).length === 0
    ) {
      // the dict is empty so it is safe to change it to an array of buffers
      setBufferSelection(targetOp, []);
    }

    // check that the buffer selection is compatible
    if (edge.type === 'bufferSeq') {
      if (!isArrayBufferSelection(bufferSelection)) {
        throw new Error(
          'a sequential buffer edge must be assigned to an array of buffers',
        );
      }
      if (!bufferSelection.includes(edge.source)) {
        bufferSelection[edge.data.seq] = edge.source;
      }
    }
    if (edge.type === 'bufferKey') {
      if (!isKeyedBufferSelection(bufferSelection)) {
        throw new Error(
          'a keyed buffer edge must be assigned to a keyed buffer selection',
        );
      }
      bufferSelection[edge.data.key] = nodeIdToNextOperation(edge.source);
    }
  }
}

function getBufferSelection(targetOp: DiagramOperation): BufferSelection {
  switch (targetOp.type) {
    case 'buffer_access':
    case 'listen':
    case 'join':
    case 'serialized_join': {
      return targetOp.buffers;
    }
    default: {
      throw new Error(`"${targetOp.type}" operation does not accept a buffer`);
    }
  }
}

function setBufferSelection(
  targetOp: DiagramOperation,
  bufferSelection: BufferSelection,
): void {
  switch (targetOp.type) {
    case 'buffer_access':
    case 'listen':
    case 'join':
    case 'serialized_join': {
      targetOp.buffers = bufferSelection;
      break;
    }
    default: {
      throw new Error(`"${targetOp.type}" operation does not accept a buffer`);
    }
  }
}

const ALLOWED_OUTPUT_EDGES: Record<NodeTypes, Set<EdgeTypes>> = {
  buffer: new Set<EdgeTypes>(['bufferKey', 'bufferSeq']),
  buffer_access: new Set<EdgeTypes>(['default']),
  fork_clone: new Set<EdgeTypes>(['default']),
  fork_result: new Set<EdgeTypes>(['forkResultOk', 'forkResultErr']),
  join: new Set<EdgeTypes>(['default']),
  listen: new Set<EdgeTypes>(['default']),
  node: new Set<EdgeTypes>(['default', 'streamOut']),
  scope: new Set<EdgeTypes>(['default', 'streamOut']),
  section: new Set<EdgeTypes>(['default']),
  serialized_join: new Set<EdgeTypes>(['default']),
  split: new Set<EdgeTypes>(['splitKey', 'splitSeq', 'splitRemaining']),
  start: new Set<EdgeTypes>(['default']),
  stream_out: new Set<EdgeTypes>([]),
  terminate: new Set<EdgeTypes>([]),
  transform: new Set<EdgeTypes>(['default']),
  unzip: new Set<EdgeTypes>(['unzip']),
};

const ALLOWED_INPUT_EDGES: Record<NodeTypes, Set<EdgeTypes>> = {
  buffer: new Set<EdgeTypes>(['default']),
  buffer_access: new Set<EdgeTypes>(['default']),
  fork_clone: new Set<EdgeTypes>(['default']),
  fork_result: new Set<EdgeTypes>(['default']),
  join: new Set<EdgeTypes>(['bufferKey', 'bufferSeq']),
  listen: new Set<EdgeTypes>(['bufferKey', 'bufferSeq']),
  node: new Set<EdgeTypes>(['default']),
  scope: new Set<EdgeTypes>(['default']),
  section: new Set<EdgeTypes>(['default']),
  serialized_join: new Set<EdgeTypes>(['bufferKey', 'bufferSeq']),
  split: new Set<EdgeTypes>(['default']),
  start: new Set<EdgeTypes>([]),
  stream_out: new Set<EdgeTypes>(['streamOut']),
  terminate: new Set<EdgeTypes>(['default']),
  transform: new Set<EdgeTypes>(['default']),
  unzip: new Set<EdgeTypes>(['default']),
};

function setIntersection<T>(a: Set<T>, b: Set<T>): Set<T> {
  const intersection = new Set<T>();
  for (const elem of a) {
    if (b.has(elem)) {
      intersection.add(elem);
    }
  }
  return intersection;
}

export function allowEdges(
  source: DiagramEditorNode,
  target: DiagramEditorNode,
): EdgeTypes[] {
  const allowed_output_edges = source.type
    ? ALLOWED_OUTPUT_EDGES[source.type]
    : new Set([]);
  const allowed_input_edges = target.type
    ? ALLOWED_INPUT_EDGES[target.type]
    : new Set([]);
  return Array.from(setIntersection(allowed_output_edges, allowed_input_edges));
}
