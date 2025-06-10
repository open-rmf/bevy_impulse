import Ajv from 'ajv';
import addFormats from 'ajv-formats';

import diagramSchema from '../diagram.schema.json';
import {
  type DiagramEditorEdge,
  type DiagramEditorNode,
  START_ID,
  TERMINATE_ID,
} from '../nodes';
import type { Diagram } from '../types/diagram';
import { buildEdges, nextOperationToNodeId } from './operation';

export interface Graph {
  startNodeId: string;
  nodes: DiagramEditorNode[];
  edges: DiagramEditorEdge[];
}

export function loadDiagramJson(jsonStr: string): Graph {
  const diagram = JSON.parse(jsonStr);
  const valid = validate(diagram);
  if (!valid) {
    throw validate.errors;
  }

  const graph = buildGraph(diagram);
  return graph;
}

function buildGraph(diagram: Diagram): Graph {
  const nodes: DiagramEditorNode[] = [
    {
      id: START_ID,
      type: 'start',
      position: { x: 0, y: 0 },
      selectable: false,
      data: {},
    },
    {
      id: TERMINATE_ID,
      type: 'terminate',
      position: { x: 0, y: 0 },
      selectable: false,
      data: {},
    },
    ...Object.entries(diagram.ops).map(
      ([opId, op]) =>
        ({
          id: opId,
          type: 'inputOutput',
          position: { x: 0, y: 0 },
          data: { opId, ...op },
        }) satisfies DiagramEditorNode,
    ),
  ];
  const startNodeId = nextOperationToNodeId(diagram.start);
  const edges: DiagramEditorEdge[] = [
    {
      id: `${START_ID}->${startNodeId}`,
      source: START_ID,
      target: startNodeId,
    },
  ];
  for (const [opId, op] of Object.entries(diagram.ops)) {
    edges.push(...buildEdges(op, opId));
  }

  return { startNodeId, nodes, edges };
}

const ajv = new Ajv();
addFormats(ajv);
ajv.addFormat('uint', /^[0-9]+$/);
const validate = ajv.compile<Diagram>(diagramSchema);
