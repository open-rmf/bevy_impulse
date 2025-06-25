import Ajv from 'ajv/dist/2020';
import addFormats from 'ajv-formats';
import { v4 as uuidv4 } from 'uuid';

import diagramSchema from '../diagram.preprocessed.schema.json';
import { START_ID, TERMINATE_ID } from '../nodes';
import type { Diagram, DiagramEditorEdge, DiagramEditorNode } from '../types';
import { buildEdges } from './operation';

export interface Graph {
  nodes: DiagramEditorNode[];
  edges: DiagramEditorEdge[];
}

export function loadDiagram(diagram: Diagram): Graph {
  const graph = buildGraph(diagram);
  return graph;
}

export function loadDiagramJson(jsonStr: string): Graph {
  const diagram = JSON.parse(jsonStr);
  const valid = validate(diagram);
  if (!valid) {
    throw validate.errors;
  }

  return loadDiagram(diagram);
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
          id: uuidv4(),
          type: op.type,
          position: { x: 0, y: 0 },
          // TODO: Support sections
          data: { namespace: '', opId, op },
        }) satisfies DiagramEditorNode,
    ),
  );
  const edges = graph.edges;
  const startNode = nodes.find((n) => n.data.opId === diagram.start);
  if (startNode) {
    edges.push({
      id: uuidv4(),
      type: 'default',
      source: START_ID,
      target: startNode.id,
      data: {},
    });
  }
  edges.push(...buildEdges(diagram, graph.nodes));

  return graph;
}

const ajv = new Ajv();
addFormats(ajv);
ajv.addFormat('uint', /^[0-9]+$/);
const validate = ajv.compile<Diagram>(diagramSchema);
