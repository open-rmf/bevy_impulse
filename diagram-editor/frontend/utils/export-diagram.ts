import type { Edge } from '@xyflow/react';
import {
  type DiagramEditorNode,
  extractOperation,
  isOperationNode,
  START_ID,
} from '../nodes';
import type { Diagram } from '../types/diagram';

export function exportDiagram(nodes: DiagramEditorNode[], edges: Edge[]) {
  const diagram: Diagram = {
    $schema:
      'https://raw.githubusercontent.com/open-rmf/bevy_impulse/refs/heads/main/diagram.schema.json',
    version: '0.1.0',
    start: '',
    ops: {},
  };

  for (const node of nodes) {
    if (isOperationNode(node)) {
      diagram.ops[node.id] = extractOperation(node);
    }
  }

  // only need to process the start edge, the diagram connections should always be in sync with the edges.
  const startEdge = edges.find((edge) => edge.source === START_ID);
  if (startEdge) {
    diagram.start = startEdge.target;
  }

  return diagram;
}
