import {
  type DiagramEditorEdge,
  type DiagramEditorNode,
  START_ID,
  TERMINATE_ID,
} from '../nodes';
import { exhaustiveCheck } from './exhaustive';

/**
 * Adds a connection to a node data.
 * Nothing will be changed if the edge is invalid. But an error will be thrown if the source node is invalid.
 * This is idempotent, if the connection exists, it will not do anything.
 */
export function addConnection(
  fromNode: DiagramEditorNode,
  edge: DiagramEditorEdge,
): void {
  if (fromNode.id === START_ID) {
    throw new Error('source node cannot be "start"');
  }
  if (fromNode.id === TERMINATE_ID) {
    throw new Error('source node cannot be "terminate"');
  }

  switch (fromNode.data.type) {
    case 'node':
    case 'join':
    case 'serialized_join':
    case 'transform':
    case 'buffer_access':
    case 'listen': {
      fromNode.data.next = edge.target;
      break;
    }
    case 'section': {
      throw new Error('TODO');
    }
    case 'fork_clone': {
      if (!fromNode.data.next.includes(edge.target)) {
        fromNode.data.next.push(edge.target);
      }
      break;
    }
    case 'unzip': {
      if (edge.data?.type !== 'unzip') {
        break;
      }
      fromNode.data.next[edge.data.seq] = edge.target;
      break;
    }
    case 'fork_result': {
      switch (edge.data?.type) {
        case 'ok': {
          fromNode.data.ok = edge.target;
          break;
        }
        case 'err': {
          fromNode.data.err = edge.target;
          break;
        }
      }
      break;
    }
    case 'split': {
      switch (edge.data?.type) {
        case 'splitKey': {
          if (!fromNode.data.keyed) {
            fromNode.data.keyed = {};
          }
          fromNode.data.keyed[edge.data.key] = edge.target;
          break;
        }
        case 'splitSequential': {
          if (!fromNode.data.sequential) {
            fromNode.data.sequential = [];
          }
          // this works because js allows non-sequential arrays
          fromNode.data.sequential[edge.data.seq] = edge.target;
          break;
        }
        case 'splitRemaining': {
          fromNode.data.remaining = edge.target;
          break;
        }
      }
      break;
    }
    case 'buffer': {
      throw new Error('buffer operations cannot have connections');
    }
    default: {
      exhaustiveCheck(fromNode.data);
    }
  }
}
