import type { DiagramEditorEdge } from '../edges';
import type { NodeManager } from '../node-manager';
import { isBuiltinNode, isOperationNode } from '../nodes';
import type { Diagram, DiagramOperation, NextOperation } from '../types/api';
import { splitNamespaces } from '../utils';

interface SubOperations {
  start: NextOperation;
  ops: Record<string, DiagramOperation>;
}

function getSubOperations(diagram: Diagram, namespace: string): SubOperations {
  const namespaces = splitNamespaces(namespace);
  let subOps: SubOperations = diagram;
  for (const namespace of namespaces.slice(1)) {
    const scopeOp = subOps.ops[namespace];
    if (!scopeOp || scopeOp.type !== 'scope') {
      throw new Error(`expected ${namespace} to be a scope operation`);
    }
    subOps = scopeOp;
  }
  return subOps;
}

export function exportDiagram(
  nodeManager: NodeManager,
  edges: DiagramEditorEdge[],
): Diagram {
  const diagram: Diagram = {
    $schema:
      'https://raw.githubusercontent.com/open-rmf/bevy_impulse/refs/heads/main/diagram.schema.json',
    version: '0.1.0',
    start: { builtin: 'dispose' },
    ops: {},
  };

  for (const node of nodeManager.iterNodes()) {
    const subOps = getSubOperations(diagram, node.data.namespace);

    if (isOperationNode(node)) {
      subOps.ops[node.data.opId] = node.data.op;
    }
  }

  for (const edge of edges) {
    const source = nodeManager.getNode(edge.source);
    if (source.type === 'start') {
      const subOps = getSubOperations(diagram, source.data.namespace);
      const target = nodeManager.getNode(edge.target);
      if (isOperationNode(target)) {
        subOps.start = target.data.opId;
      } else if (isBuiltinNode(target)) {
        subOps.start = { builtin: target.type };
      } else {
        throw new Error('unknown node');
      }
    }
    nodeManager.syncEdge(edge);
  }

  return diagram;
}
