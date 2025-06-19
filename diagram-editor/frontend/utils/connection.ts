import { START_ID, TERMINATE_ID } from '../nodes';
import type {
  BufferSelection,
  Diagram,
  DiagramEditorEdge,
  DiagramOperation,
} from '../types';
import { exhaustiveCheck } from './exhaustive-check';
import {
  isArrayBufferSelection,
  isKeyedBufferSelection,
  nextOperationToNodeId,
  nodeIdToNextOperation,
} from './operation';

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
        sourceOp.stream_out = sourceOp.stream_out ? sourceOp.stream_out : {};
        sourceOp.stream_out[edge.data.name] = nodeIdToNextOperation(
          edge.target,
        );
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
      if (edge.type !== 'default') {
        throw new Error('scope operation must have default edge');
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
