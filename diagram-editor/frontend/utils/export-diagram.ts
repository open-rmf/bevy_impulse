import equal from 'fast-deep-equal';
import type { DiagramEditorEdge, StreamOutEdge } from '../edges';
import type { NodeManager } from '../node-manager';
import { isOperationNode } from '../nodes';
import type {
  BufferSelection,
  Diagram,
  DiagramOperation,
  NextOperation,
  SectionTemplate,
} from '../types/api';
import {
  exhaustiveCheck,
  isArrayBufferSelection,
  isKeyedBufferSelection,
  isScopeNode,
  splitNamespaces,
} from '../utils';

interface SubOperations {
  start: NextOperation;
  ops: Record<string, DiagramOperation>;
}

function getSubOperations(
  root: SubOperations,
  namespace: string,
): SubOperations {
  const namespaces = splitNamespaces(namespace);
  let subOps: SubOperations = root;
  for (const namespace of namespaces.slice(1)) {
    const scopeOp = subOps.ops[namespace];
    if (!scopeOp || scopeOp.type !== 'scope') {
      throw new Error(`expected ${namespace} to be a scope operation`);
    }
    subOps = scopeOp;
  }
  return subOps;
}

function syncStreamOut(
  nodeManager: NodeManager,
  sourceOp: Extract<DiagramOperation, { type: 'node' | 'scope' }>,
  edge: StreamOutEdge,
) {
  sourceOp.stream_out = sourceOp.stream_out ? sourceOp.stream_out : {};
  sourceOp.stream_out[edge.data.name] = nodeManager.getTargetNextOp(edge);
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

function syncBufferSelection(
  nodeManager: NodeManager,
  edge: DiagramEditorEdge,
) {
  if (edge.type === 'bufferKey' || edge.type === 'bufferSeq') {
    const targetNode = nodeManager.getNode(edge.target);
    if (!isOperationNode(targetNode)) {
      throw new Error('expected operation node');
    }
    const targetOp = targetNode.data.op;
    if (!targetOp) {
      throw new Error(`target operation "${edge.target}" not found`);
    }
    let bufferSelection = getBufferSelection(targetOp);

    if (
      edge.type === 'bufferKey' &&
      Array.isArray(bufferSelection) &&
      bufferSelection.length === 0
    ) {
      // the array is empty so it is safe to change it to a keyed buffer selection
      bufferSelection = {};
      setBufferSelection(targetOp, bufferSelection);
    } else if (
      edge.type === 'bufferSeq' &&
      typeof bufferSelection === 'object' &&
      !Array.isArray(bufferSelection) &&
      Object.keys(bufferSelection).length === 0
    ) {
      // the dict is empty so it is safe to change it to an array of buffers
      bufferSelection = [];
      setBufferSelection(targetOp, bufferSelection);
    }

    const sourceNode = nodeManager.getNode(edge.source);
    if (sourceNode.type !== 'buffer') {
      throw new Error('expected source to be a buffer node');
    }
    // check that the buffer selection is compatible
    if (edge.type === 'bufferSeq') {
      if (!isArrayBufferSelection(bufferSelection)) {
        throw new Error(
          'a sequential buffer edge must be assigned to an array of buffers',
        );
      }
      bufferSelection[edge.data.seq] = sourceNode.data.opId;
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
function syncEdge(
  nodeManager: NodeManager,
  root: SubOperations,
  edge: DiagramEditorEdge,
): void {
  if (edge.type === 'bufferKey' || edge.type === 'bufferSeq') {
    syncBufferSelection(nodeManager, edge);
    return;
  }

  const sourceNode = nodeManager.getNode(edge.source);

  if (sourceNode.type === 'start') {
    const subOps: SubOperations = (() => {
      if (!sourceNode.parentId) {
        return root;
      }
      const scopeNode = nodeManager.getNode(sourceNode.parentId);
      if (!isScopeNode(scopeNode)) {
        throw new Error('expected parent to be a scope node');
      }
      return scopeNode.data.op;
    })();
    const target = nodeManager.getTargetNextOp(edge);
    subOps.start = target;
  }

  if (!isOperationNode(sourceNode)) {
    return;
  }
  const sourceOp = sourceNode.data.op;

  switch (sourceOp.type) {
    case 'node': {
      if (edge.type === 'streamOut') {
        syncStreamOut(nodeManager, sourceOp, edge);
      } else if (edge.type === 'default') {
        sourceOp.next = nodeManager.getTargetNextOp(edge);
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

      sourceOp.next = nodeManager.getTargetNextOp(edge);
      break;
    }
    case 'section': {
      throw new Error('TODO');
    }
    case 'fork_clone': {
      if (edge.type !== 'default') {
        throw new Error('expected "default" edge');
      }

      const newNextOp = nodeManager.getTargetNextOp(edge);
      if (!sourceOp.next.some((next) => equal(next, newNextOp))) {
        sourceOp.next.push(newNextOp);
      }
      break;
    }
    case 'unzip': {
      if (edge.type !== 'unzip') {
        throw new Error('expected "unzip" edge');
      }
      sourceOp.next[edge.data.seq] = nodeManager.getTargetNextOp(edge);
      break;
    }
    case 'fork_result': {
      switch (edge.type) {
        case 'forkResultOk': {
          sourceOp.ok = nodeManager.getTargetNextOp(edge);
          break;
        }
        case 'forkResultErr': {
          sourceOp.err = nodeManager.getTargetNextOp(edge);
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
          sourceOp.keyed[edge.data.key] = nodeManager.getTargetNextOp(edge);
          break;
        }
        case 'splitSeq': {
          if (!sourceOp.sequential) {
            sourceOp.sequential = [];
          }
          // this works because js allows non-sequential arrays
          sourceOp.sequential[edge.data.seq] =
            nodeManager.getTargetNextOp(edge);
          break;
        }
        case 'splitRemaining': {
          sourceOp.remaining = nodeManager.getTargetNextOp(edge);
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
        syncStreamOut(nodeManager, sourceOp, edge);
      } else if (edge.type !== 'default') {
        throw new Error('scope operation must have default or streamOut edge');
      }
      sourceOp.next = nodeManager.getTargetNextOp(edge);
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

/**
 * Update the operation connections from the edges.
 *
 * @param root only used to update the `start` field, does not actually populate the operations.
 */
function syncEdges(
  nodeManager: NodeManager,
  root: SubOperations,
  edges: DiagramEditorEdge[],
): void {
  // first clear all the connections
  root.start = { builtin: 'dispose' };
  for (const node of nodeManager.iterNodes()) {
    if (!isOperationNode(node)) {
      continue;
    }

    switch (node.data.op.type) {
      case 'node': {
        node.data.op.next = { builtin: 'dispose' };
        delete node.data.op.stream_out;
        break;
      }
      case 'scope': {
        node.data.op.next = { builtin: 'dispose' };
        delete node.data.op.stream_out;
        node.data.op.start = { builtin: 'dispose' };
        break;
      }
      case 'fork_clone':
      case 'unzip': {
        node.data.op.next = [];
        break;
      }
      case 'transform': {
        node.data.op.next = { builtin: 'dispose' };
        break;
      }
      case 'join':
      case 'serialized_join':
      case 'listen':
      case 'buffer_access': {
        node.data.op.next = { builtin: 'dispose' };
        node.data.op.buffers = [];
        break;
      }
      case 'section': {
        node.data.op.connect = {};
        break;
      }
      case 'fork_result': {
        node.data.op.ok = { builtin: 'dispose' };
        node.data.op.err = { builtin: 'dispose' };
        break;
      }
      case 'split': {
        delete node.data.op.keyed;
        delete node.data.op.sequential;
        delete node.data.op.remaining;
        break;
      }
      case 'buffer': {
        break;
      }
      case 'stream_out': {
        break;
      }
      default: {
        exhaustiveCheck(node.data.op);
      }
    }
  }

  for (const edge of edges) {
    syncEdge(nodeManager, root, edge);
  }
}

export function exportDiagram(
  nodeManager: NodeManager,
  edges: DiagramEditorEdge[],
  templates: Record<string, SectionTemplate>,
): Diagram {
  const diagram: Diagram = {
    $schema:
      'https://raw.githubusercontent.com/open-rmf/bevy_impulse/refs/heads/main/diagram.schema.json',
    version: '0.1.0',
    templates,
    start: { builtin: 'dispose' },
    ops: {},
  };

  syncEdges(nodeManager, diagram, edges);

  // visit the nodes breath first to ensure that the parents exist in the diagram before
  // populating the children.
  const sortedNodes = Array.from(nodeManager.iterNodes()).sort(
    (a, b) =>
      splitNamespaces(a.data.namespace).length -
      splitNamespaces(b.data.namespace).length,
  );
  for (const node of sortedNodes) {
    const subOps = getSubOperations(diagram, node.data.namespace);

    if (isOperationNode(node)) {
      subOps.ops[node.data.opId] = { ...node.data.op };
      if (node.data.op.type === 'scope') {
        // do not carry over stale ops from the node data
        subOps.ops[node.data.opId].ops = {};
      }
    }
  }

  return diagram;
}

export function exportTemplate(
  nodeManager: NodeManager,
  edges: DiagramEditorEdge[],
): SectionTemplate {
  const template = {
    inputs: {} as Record<string, NextOperation>,
    outputs: [] as string[],
    buffers: {} as Record<string, NextOperation>,
    ops: {},
  } satisfies Required<SectionTemplate>;
  const fakeRoot: SubOperations = {
    start: { builtin: 'dispose' },
    ops: {},
  };

  syncEdges(nodeManager, fakeRoot, edges);

  // visit the nodes breath first to ensure that the parents exist in the diagram before
  // populating the children.
  const sortedNodes = Array.from(nodeManager.iterNodes()).sort(
    (a, b) =>
      splitNamespaces(a.data.namespace).length -
      splitNamespaces(b.data.namespace).length,
  );
  for (const node of sortedNodes) {
    const subOps = getSubOperations(fakeRoot, node.data.namespace);

    if (isOperationNode(node)) {
      subOps.ops[node.data.opId] = { ...node.data.op };
      if (node.data.op.type === 'scope') {
        // do not carry over stale ops from the node data
        subOps.ops[node.data.opId].ops = {};
      }
    } else if (node.type === 'sectionInput') {
      template.inputs[node.data.remappedId] = node.data.targetId;
    } else if (node.type === 'sectionOutput') {
      template.outputs.push(node.data.remappedId);
    } else if (node.type === 'sectionBuffer') {
      template.buffers[node.data.remappedId] = node.data.targetId;
    }
  }

  return {
    inputs: {},
    outputs: [],
    buffers: {},
    ops: fakeRoot.ops,
  };
}
