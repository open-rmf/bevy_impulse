import type { Diagram, DiagramEditorEdge, DiagramEditorNode } from '..';
import { extractOperation, isOperationNode } from '..';
import { syncEdge } from './connection';

export function exportDiagram(
  nodes: DiagramEditorNode[],
  edges: DiagramEditorEdge[],
): Diagram {
  const diagram: Diagram = {
    $schema:
      'https://raw.githubusercontent.com/open-rmf/bevy_impulse/refs/heads/main/diagram.schema.json',
    version: '0.1.0',
    start: { builtin: 'dispose' },
    ops: {},
  };

  for (const node of nodes) {
    if (isOperationNode(node)) {
      diagram.ops[node.id] = extractOperation(node);
    }
  }

  for (const edge of edges) {
    syncEdge(diagram, edge);
  }

  return diagram;
}
