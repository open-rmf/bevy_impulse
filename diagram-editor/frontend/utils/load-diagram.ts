import addFormats from 'ajv-formats';
import Ajv from 'ajv/dist/2020';

import diagramSchema from '../diagram.schema.json';
import { START_ID, TERMINATE_ID } from '../nodes';
import type { Diagram, DiagramEditorEdge, DiagramEditorNode } from '../types';
import { buildEdges, nextOperationToNodeId } from './operation';

export interface Graph {
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

export function loadEmpty(): Graph {
  return {
    nodes: [
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
        position: { x: 0, y: 400 },
        selectable: false,
        data: {},
      },
    ],
    edges: [],
  };
}

function buildGraph(diagram: Diagram): Graph {
  const graph = loadEmpty();
  const nodes = graph.nodes;
  nodes.push(
    ...Object.entries(diagram.ops).map(
      ([opId, op]) =>
        ({
          id: opId,
          type: op.type,
          position: { x: 0, y: 0 },
          data: { opId, ...op },
        }) satisfies DiagramEditorNode,
    ),
  );
  const edges = graph.edges;
  const startNodeId = nextOperationToNodeId(diagram.start);
  if (startNodeId) {
    edges.push({
      id: `${START_ID}->${startNodeId}`,
      type: 'default',
      source: START_ID,
      target: startNodeId,
      data: {},
    });
  }
  for (const [opId, op] of Object.entries(diagram.ops)) {
    edges.push(...buildEdges(op, opId));
  }

  return graph;
}

const ajv = new Ajv();
addFormats(ajv);
ajv.addFormat('uint', /^[0-9]+$/);
const validate = ajv.compile<Diagram>(diagramSchema);
