import type { NodeManager } from '../node-manager';
import { START_ID } from '../nodes';
import type { Diagram, DiagramEditorEdge } from '../types';
import {
  isBuiltinNode,
  isOperationNode,
  joinNamespaces,
  splitNamespaces,
} from '../utils';

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
    if (isOperationNode(node)) {
      const namespaces = splitNamespaces(node.data.namespace);
      let ops = diagram.ops;
      for (const namespace of namespaces.slice(1)) {
        const scopeOp = ops[namespace];
        if (!scopeOp || scopeOp.type !== 'scope') {
          throw new Error(`expected ${namespace} to be a scope operation`);
        }
        ops = scopeOp.ops;
      }
      ops[node.data.opId] = node.data.op;
    }
  }

  for (const edge of edges) {
    if (edge.source === joinNamespaces('', START_ID)) {
      const node = nodeManager.getNode(edge.target);
      if (isOperationNode(node)) {
        diagram.start = node.data.opId;
      } else if (isBuiltinNode(node)) {
        diagram.start = { builtin: node.type };
      } else {
        throw new Error('unknown node');
      }
    }
    nodeManager.syncEdge(edge);
  }

  return diagram;
}
