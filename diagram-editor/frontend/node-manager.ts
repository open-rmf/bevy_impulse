import equal from 'fast-deep-equal';
import { TERMINATE_ID } from './nodes';
import type {
  BufferSelection,
  DiagramEditorEdge,
  DiagramEditorNode,
  DiagramOperation,
  NextOperation,
  StreamOutEdge,
} from './types';
import {
  exhaustiveCheck,
  isArrayBufferSelection,
  isBuiltin,
  isBuiltinNode,
  isKeyedBufferSelection,
} from './utils';

function joinNamespaceOpId(a: string, b: string) {
  return `${a}:${b}`;
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

export class NodeManager {
  constructor(public nodes: DiagramEditorNode[]) {
    // TODO: build a lookup map to improve performance
  }

  getNode(nodeId: string): DiagramEditorNode {
    const node = this.nodes.find((n) => n.id === nodeId);
    if (!node) {
      throw new Error(`cannot find node "${nodeId}"`);
    }
    return node;
  }

  hasNode(nodeId: string): boolean {
    try {
      this.getNode(nodeId);
      return true;
    } catch {
      return false;
    }
  }

  getNodeFromNamespaceOpId(namespace: string, opId: string): DiagramEditorNode {
    const node = this.nodes.find(
      (n) => n.data.namespace === namespace && n.data.opId === opId,
    );
    if (!node) {
      throw new Error(
        `cannot find node for operation "${joinNamespaceOpId(namespace, opId)}"`,
      );
    }
    return node;
  }

  getNodeFromRootOpId(opId: string): DiagramEditorNode {
    return this.getNodeFromNamespaceOpId('', opId);
  }

  getNodeFromNextOp(
    fromNode: DiagramEditorNode,
    nextOp: NextOperation,
  ): DiagramEditorNode | null {
    if (isBuiltin(nextOp)) {
      switch (nextOp.builtin) {
        case 'dispose':
        case 'cancel':
          return null;
        case 'terminate':
          return this.getNode(TERMINATE_ID);
      }
    }

    const [namespace, opId] = (() => {
      if (typeof nextOp === 'object') {
        const [namespace, opId] = Object.entries(nextOp)[0];
        return [joinNamespaceOpId(fromNode.data.namespace, namespace), opId];
      }
      return [fromNode.data.namespace, nextOp];
    })();

    const node = this.nodes.find(
      (n) => n.data.namespace === namespace && n.data.opId === opId,
    );
    if (!node) {
      throw new Error(
        `cannot find operation ${joinNamespaceOpId(namespace, opId)}`,
      );
    }
    return node;
  }

  getTargetNextOp(edge: DiagramEditorEdge): NextOperation {
    // TODO: Validate that the edge does not traverse namespaces
    switch (edge.type) {
      case 'bufferKey':
      case 'bufferSeq':
      case 'default':
      case 'forkResultOk':
      case 'forkResultErr':
      case 'splitKey':
      case 'splitSeq':
      case 'splitRemaining':
      case 'streamOut':
      case 'unzip': {
        const target = this.getNode(edge.target);
        if (isBuiltinNode(target)) {
          return { builtin: target.type };
        }
        return target.data.opId;
      }
      // TODO: For section edges, return a `{ [target.data.opId]: edge.data.input }`
      default: {
        exhaustiveCheck(edge);
        throw new Error('unknown edge');
      }
    }
  }

  private syncStreamOut(
    sourceOp: Extract<DiagramOperation, { type: 'node' | 'scope' }>,
    edge: StreamOutEdge,
  ) {
    sourceOp.stream_out = sourceOp.stream_out ? sourceOp.stream_out : {};
    sourceOp.stream_out[edge.data.name] = this.getTargetNextOp(edge);
  }

  private syncBufferSelection(edge: DiagramEditorEdge) {
    if (edge.type === 'bufferKey' || edge.type === 'bufferSeq') {
      const targetNode = this.getNode(edge.target);
      const targetOp = targetNode.data.op;
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

      const sourceNode = this.getNode(edge.source);
      // check that the buffer selection is compatible
      if (edge.type === 'bufferSeq') {
        if (!isArrayBufferSelection(bufferSelection)) {
          throw new Error(
            'a sequential buffer edge must be assigned to an array of buffers',
          );
        }
        if (!bufferSelection.includes(edge.source)) {
          bufferSelection[edge.data.seq] = sourceNode.data.opId;
        }
      }
      if (edge.type === 'bufferKey') {
        if (!isKeyedBufferSelection(bufferSelection)) {
          throw new Error(
            'a keyed buffer edge must be assigned to a keyed buffer selection',
          );
        }
        bufferSelection[edge.data.key] = sourceNode.data.opId;
      }
    }
  }

  /**
   * Update a node's data with the edge, this updates fields like `next` and `buffer` to be
   * in sync with the edge data.
   */
  syncEdge(edge: DiagramEditorEdge): void {
    if (edge.source === TERMINATE_ID) {
      throw new Error('source node cannot be "terminate"');
    }

    if (edge.type === 'bufferKey' || edge.type === 'bufferSeq') {
      this.syncBufferSelection(edge);
      return;
    }

    const sourceNode = this.getNode(edge.source);
    if (isBuiltinNode(sourceNode)) {
      return;
    }
    const sourceOp = sourceNode.data.op;

    switch (sourceOp.type) {
      case 'node': {
        if (edge.type === 'streamOut') {
          this.syncStreamOut(sourceOp, edge);
        } else if (edge.type === 'default') {
          sourceOp.next = this.getTargetNextOp(edge);
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

        sourceOp.next = this.getTargetNextOp(edge);
        break;
      }
      case 'section': {
        throw new Error('TODO');
      }
      case 'fork_clone': {
        if (edge.type !== 'default') {
          throw new Error('expected "default" edge');
        }

        const newNextOp = this.getTargetNextOp(edge);
        if (!sourceOp.next.some((next) => equal(next, newNextOp))) {
          sourceOp.next.push(newNextOp);
        }
        break;
      }
      case 'unzip': {
        if (edge.type !== 'unzip') {
          throw new Error('expected "unzip" edge');
        }
        sourceOp.next[edge.data.seq] = this.getTargetNextOp(edge);
        break;
      }
      case 'fork_result': {
        switch (edge.type) {
          case 'forkResultOk': {
            sourceOp.ok = this.getTargetNextOp(edge);
            break;
          }
          case 'forkResultErr': {
            sourceOp.err = this.getTargetNextOp(edge);
            break;
          }
          default: {
            throw new Error(
              'fork_result operation must have "ok" or "err" edge',
            );
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
            sourceOp.keyed[edge.data.key] = this.getTargetNextOp(edge);
            break;
          }
          case 'splitSeq': {
            if (!sourceOp.sequential) {
              sourceOp.sequential = [];
            }
            // this works because js allows non-sequential arrays
            sourceOp.sequential[edge.data.seq] = this.getTargetNextOp(edge);
            break;
          }
          case 'splitRemaining': {
            sourceOp.remaining = this.getTargetNextOp(edge);
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
          this.syncStreamOut(sourceOp, edge);
        } else if (edge.type !== 'default') {
          throw new Error(
            'scope operation must have default or streamOut edge',
          );
        }
        sourceOp.next = this.getTargetNextOp(edge);
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
}
