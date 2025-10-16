import equal from 'fast-deep-equal';
import {
  BufferFetchType,
  type DiagramEditorEdge,
  type StreamOutEdge,
} from '../edges';
import type { NodeManager } from '../node-manager';
import {
  type DiagramEditorNode,
  isBuiltinNode,
  isOperationNode,
  isScopeNode,
} from '../nodes';
import type {
  BufferSelection,
  Diagram,
  DiagramElementRegistry,
  DiagramOperation,
  NextOperation,
  SectionTemplate,
} from '../types/api';
import { exhaustiveCheck } from './exhaustive-check';
import { ROOT_NAMESPACE, splitNamespaces } from './namespace';
import { isArrayBufferSelection, isKeyedBufferSelection } from './operation';

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
  sourceOp.stream_out[edge.data.output.streamId] =
    nodeManager.getTargetNextOp(edge);
}

function getBufferSelection(targetOp: DiagramOperation): BufferSelection {
  switch (targetOp.type) {
    case 'buffer_access':
    case 'listen':
    case 'join': {
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
    case 'join': {
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
  if (edge.type === 'buffer') {
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
      edge.data.input?.type === 'bufferKey' &&
      Array.isArray(bufferSelection) &&
      bufferSelection.length === 0
    ) {
      // the array is empty so it is safe to change it to a keyed buffer selection
      bufferSelection = {};
      setBufferSelection(targetOp, bufferSelection);
    } else if (
      edge.data.input?.type === 'bufferSeq' &&
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
    if (edge.type === 'buffer' && edge.data.input?.type === 'bufferSeq') {
      if (!isArrayBufferSelection(bufferSelection)) {
        throw new Error(
          'a sequential buffer edge must be assigned to an array of buffers',
        );
      }
      bufferSelection[edge.data.input.seq] = sourceNode.data.opId;
    }
    if (edge.type === 'buffer' && edge.data.input?.type === 'bufferKey') {
      if (!isKeyedBufferSelection(bufferSelection)) {
        throw new Error(
          'a keyed buffer edge must be assigned to a keyed buffer selection',
        );
      }
      bufferSelection[edge.data.input.key] = sourceNode.data.opId;
    }

    if (targetOp.type === 'join') {
      if (edge.data.input.fetchType === BufferFetchType.Clone) {
        if (!targetOp.clone) {
          targetOp.clone = [];
        }
        if (edge.data.input?.type === 'bufferSeq') {
          targetOp.clone.push(edge.data.input.seq);
        }
        if (edge.data.input?.type === 'bufferKey') {
          targetOp.clone.push(edge.data.input.key);
        }
      }
    }
  }
}

function setSequentialKey(
  sequences: NextOperation[],
  idx: number,
  value: NextOperation,
) {
  while (sequences.length <= idx) {
    sequences.push({ builtin: 'dispose' });
  }
  sequences[idx] = value;
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
  if (edge.type === 'buffer') {
    syncBufferSelection(nodeManager, edge);
    return;
  }

  const sourceNode = nodeManager.getNode(edge.source);

  if (isOperationNode(sourceNode)) {
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
        if (edge.type !== 'section') {
          throw new Error('expected section edge');
        }

        if (!sourceOp.connect) {
          sourceOp.connect = {};
        }
        sourceOp.connect[edge.data.output.output] =
          nodeManager.getTargetNextOp(edge);
        break;
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
        sourceOp.next[edge.data.output.seq] = nodeManager.getTargetNextOp(edge);
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
            sourceOp.keyed[edge.data.output.key] =
              nodeManager.getTargetNextOp(edge);
            break;
          }
          case 'splitSeq': {
            if (!sourceOp.sequential) {
              sourceOp.sequential = [];
            }
            const next = nodeManager.getTargetNextOp(edge);
            setSequentialKey(sourceOp.sequential, edge.data.output.seq, next);
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
          throw new Error(
            'scope operation must have default or streamOut edge',
          );
        }
        sourceOp.next = nodeManager.getTargetNextOp(edge);
        break;
      }
      case 'stream_out': {
        break;
      }
      default: {
        exhaustiveCheck(sourceOp);
        throw new Error('unknown operation type');
      }
    }
  } else if (sourceNode.type === 'start') {
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
  } else if (
    sourceNode.type === 'sectionInput' ||
    sourceNode.type === 'sectionBuffer'
  ) {
    sourceNode.data.targetId = nodeManager.getTargetNextOp(edge);
  }
}

function clearConnections(nodeManager: NodeManager, root: SubOperations) {
  root.start = { builtin: 'dispose' };
  for (const node of nodeManager.iterNodes()) {
    switch (node.type) {
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
      case 'join': {
        node.data.op.next = { builtin: 'dispose' };
        node.data.op.buffers = [];
        delete node.data.op.clone;
        break;
      }
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
      case 'sectionInput':
      case 'sectionBuffer': {
        node.data.targetId = { builtin: 'dispose' };
        break;
      }
      case 'sectionOutput':
      case 'start':
      case 'terminate':
        break;
      default: {
        exhaustiveCheck(node);
      }
    }
  }
}

/**
 * Update the operation connections from the edges.
 *
 * @param root only used to update the `start` field, does not actually populate the operations.
 */
function syncEdges(
  registry: DiagramElementRegistry,
  nodeManager: NodeManager,
  root: SubOperations,
  edges: DiagramEditorEdge[],
): void {
  // first clear all the connections
  clearConnections(nodeManager, root);

  const validEdges = edges.filter((edge) => {
    // Filter out zombie stream edges that connects to a node without streams
    // Reactflow does not synchronous the `Node.handles` property to the actual handles of a node,
    // so we need to rely on the registry to cross reference.
    if (edge.type === 'streamOut') {
      const sourceNode = nodeManager.getNode(edge.source);
      const builderMetadata =
        isOperationNode(sourceNode) && sourceNode.data.op.type === 'node'
          ? registry.nodes[sourceNode.data.op.builder]
          : null;
      const hasStreams = builderMetadata?.streams
        ? Object.keys(builderMetadata.streams).length > 0
        : false;
      return hasStreams;
    }

    return true;
  });

  for (const edge of validEdges) {
    syncEdge(nodeManager, root, edge);
  }
}

/**
 * Split the node's namespace if it has one, else return `[ROOT_NAMESPACE]`.
 */
function splitNodeNamespace(node: DiagramEditorNode): string[] {
  if (isBuiltinNode(node) || isOperationNode(node)) {
    return splitNamespaces(node.data.namespace);
  }
  return [ROOT_NAMESPACE];
}

export function exportDiagram(
  registry: DiagramElementRegistry,
  nodeManager: NodeManager,
  edges: DiagramEditorEdge[],
  templates: Record<string, SectionTemplate>,
): Diagram {
  const diagram: Diagram = {
    $schema:
      'https://raw.githubusercontent.com/open-rmf/crossflow/refs/heads/main/diagram.schema.json',
    version: '0.1.0',
    templates,
    start: { builtin: 'dispose' },
    ops: {},
  };

  syncEdges(registry, nodeManager, diagram, edges);

  // visit the nodes breath first to ensure that the parents exist in the diagram before
  // populating the children.
  const sortedNodes = Array.from(nodeManager.iterNodes()).sort(
    (a, b) => splitNodeNamespace(a).length - splitNodeNamespace(b).length,
  );
  for (const node of sortedNodes) {
    if (!isOperationNode(node)) {
      continue;
    }

    const subOps = getSubOperations(diagram, node.data.namespace);
    subOps.ops[node.data.opId] = { ...node.data.op };
    if (node.data.op.type === 'scope') {
      // do not carry over stale ops from the node data
      subOps.ops[node.data.opId].ops = {};
    }
  }

  return diagram;
}

export function exportTemplate(
  registry: DiagramElementRegistry,
  nodeManager: NodeManager,
  edges: DiagramEditorEdge[],
): SectionTemplate {
  const fakeRoot: SubOperations = {
    start: { builtin: 'dispose' },
    ops: {},
  };
  const template = {
    inputs: {} as Record<string, NextOperation>,
    outputs: [] as string[],
    buffers: {} as Record<string, NextOperation>,
    ops: fakeRoot.ops,
  } satisfies Required<SectionTemplate>;

  syncEdges(registry, nodeManager, fakeRoot, edges);

  // visit the nodes breath first to ensure that the parents exist in the diagram before
  // populating the children.
  const sortedNodes = Array.from(nodeManager.iterNodes()).sort(
    (a, b) => splitNodeNamespace(a).length - splitNodeNamespace(b).length,
  );
  for (const node of sortedNodes) {
    if (isOperationNode(node)) {
      const subOps = getSubOperations(fakeRoot, node.data.namespace);
      subOps.ops[node.data.opId] = { ...node.data.op };
      if (node.data.op.type === 'scope') {
        // do not carry over stale ops from the node data
        subOps.ops[node.data.opId].ops = {};
      }
    } else if (node.type === 'sectionInput') {
      template.inputs[node.data.remappedId] = node.data.targetId;
    } else if (node.type === 'sectionOutput') {
      template.outputs.push(node.data.outputId);
    } else if (node.type === 'sectionBuffer') {
      template.buffers[node.data.remappedId] = node.data.targetId;
    }
  }

  return template;
}
