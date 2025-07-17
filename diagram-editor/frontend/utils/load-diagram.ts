import { v4 as uuidv4 } from 'uuid';
import diagramSchema from '../diagram.preprocessed.schema.json';
import { START_ID, TERMINATE_ID } from '../nodes';
import type {
  Diagram,
  DiagramEditorEdge,
  DiagramEditorNode,
  DiagramOperation,
} from '../types';
import ajv from './ajv';
import { LAYOUT_OPTIONS } from './layout';
import { joinNamespaces, ROOT_NAMESPACE } from './namespace';
import { buildEdges, isBuiltin, isOperationNode } from './operation';

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
        id: joinNamespaces(ROOT_NAMESPACE, START_ID),
        type: 'start',
        position: { x: 0, y: 0 },
        selectable: false,
        data: { namespace: ROOT_NAMESPACE },
        width: LAYOUT_OPTIONS.nodeWidth,
        height: LAYOUT_OPTIONS.nodeHeight,
      },
      {
        id: joinNamespaces(ROOT_NAMESPACE, TERMINATE_ID),
        type: 'terminate',
        position: { x: 0, y: 400 },
        selectable: false,
        data: { namespace: ROOT_NAMESPACE },
        width: LAYOUT_OPTIONS.nodeWidth,
        height: LAYOUT_OPTIONS.nodeHeight,
      },
    ],
    edges: [],
  };
}

function buildGraph(diagram: Diagram): Graph {
  const graph = loadEmpty();
  const nodes = graph.nodes;

  interface State {
    parentId?: string;
    namespace: string;
    opId: string;
    op: DiagramOperation;
  }

  const stack = [
    ...Object.entries(diagram.ops).map(
      ([opId, op]) =>
        ({ parentId: undefined, namespace: ROOT_NAMESPACE, opId, op }) as State,
    ),
  ];

  for (let state = stack.pop(); state !== undefined; state = stack.pop()) {
    const { parentId, namespace, opId, op } = state;

    if (op.type === 'scope') {
      const id = uuidv4();
      nodes.push({
        id,
        type: 'scope',
        position: { x: 0, y: 0 },
        data: { namespace, opId, op },
        parentId,
        width: 0,
        height: 0,
        zIndex: -10,
      });
      nodes.push({
        id: joinNamespaces(namespace, opId, START_ID),
        type: 'start',
        position: { x: 0, y: 0 },
        data: { namespace: joinNamespaces(namespace, opId) },
        parentId: id,
        width: LAYOUT_OPTIONS.nodeWidth,
        height: LAYOUT_OPTIONS.nodeHeight,
      });
      nodes.push({
        id: joinNamespaces(namespace, opId, TERMINATE_ID),
        type: 'terminate',
        position: { x: 0, y: 0 },
        data: { namespace: joinNamespaces(namespace, opId) },
        parentId: id,
        width: LAYOUT_OPTIONS.nodeWidth,
        height: LAYOUT_OPTIONS.nodeHeight,
      });

      for (const [innerOpId, innerOp] of Object.entries(op.ops)) {
        stack.push({
          parentId: id,
          namespace: joinNamespaces(namespace, opId),
          opId: innerOpId,
          op: innerOp,
        });
      }
    } else {
      nodes.push({
        id: uuidv4(),
        type: op.type,
        position: { x: 0, y: 0 },
        data: { namespace, opId, op },
        parentId,
        width: LAYOUT_OPTIONS.nodeWidth,
        height: LAYOUT_OPTIONS.nodeHeight,
      });
    }
  }

  const edges = graph.edges;
  const diagramStart = diagram.start;
  const startNode = isBuiltin(diagramStart)
    ? nodes.find((n) => n.type === diagramStart.builtin)
    : nodes.find(
        (n) =>
          isOperationNode(n) &&
          n.data.namespace === ROOT_NAMESPACE &&
          n.data.opId === diagramStart,
      );
  if (startNode) {
    edges.push({
      id: uuidv4(),
      type: 'default',
      source: joinNamespaces(ROOT_NAMESPACE, START_ID),
      target: startNode.id,
      data: {},
    });
  }
  edges.push(...buildEdges(diagram, graph.nodes));

  return graph;
}

const validate = ajv.compile<Diagram>(diagramSchema);
