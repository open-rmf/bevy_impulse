import type { NodeManager } from '../node-manager';
import { START_ID } from '../nodes';
import type { Diagram, DiagramEditorEdge } from '../types';
import { isOperationNode } from '../utils';

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
      diagram.ops[node.data.opId] = node.data.op;
    }
  }

  for (const edge of edges) {
    if (edge.source === START_ID) {
      diagram.start = nodeManager.getNode(edge.target).data.opId;
    }
    nodeManager.syncEdge(edge);
  }

  return diagram;
}
