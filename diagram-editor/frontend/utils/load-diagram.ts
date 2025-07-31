import { createDefaultEdge, type DiagramEditorEdge } from '../edges';
import { NodeManager } from '../node-manager';
import {
  createOperationNode,
  createScopeNode,
  createSectionBufferNode,
  createSectionInputNode,
  createSectionOutputNode,
  createStartNode,
  createTerminateNode,
  type DiagramEditorNode,
  isOperationNode,
  START_ID,
} from '../nodes';
import type { Diagram, DiagramOperation, SectionTemplate } from '../types/api';
import { getSchema } from './ajv';
import { exportDiagram } from './export-diagram';
import { joinNamespaces, ROOT_NAMESPACE } from './namespace';
import { buildEdges, isBuiltin } from './operation';

export interface Graph {
  nodes: DiagramEditorNode[];
  edges: DiagramEditorEdge[];
}

export function loadDiagram(diagram: Diagram): Graph {
  const graph = buildGraph(diagram);
  return graph;
}

export function loadDiagramJson(jsonStr: string): [Diagram, Graph] {
  const diagram = JSON.parse(jsonStr);
  const valid = validate(diagram);
  if (!valid) {
    const error = validate.errors?.[0];
    if (!error) {
      throw 'unknown error';
    }
    throw `${error.instancePath} ${error.message}`;
  }

  return [diagram, loadDiagram(diagram)];
}

export function loadEmpty(): Graph {
  return {
    nodes: [
      createStartNode(ROOT_NAMESPACE, { x: 0, y: 0 }),
      createTerminateNode(ROOT_NAMESPACE, { x: 0, y: 400 }),
    ],
    edges: [],
  };
}

function buildGraph(diagram: Diagram, initialGraph?: Graph): Graph {
  const graph = initialGraph ?? loadEmpty();
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
      const scopeNodes = createScopeNode(
        namespace,
        parentId,
        { x: 0, y: 0 },
        op,
        opId,
      );
      const scopeId = scopeNodes[0].id;
      nodes.push(...scopeNodes);

      for (const [innerOpId, innerOp] of Object.entries(op.ops)) {
        stack.push({
          parentId: scopeId,
          namespace: joinNamespaces(namespace, opId),
          opId: innerOpId,
          op: innerOp,
        });
      }
    } else {
      nodes.push(
        createOperationNode(namespace, parentId, { x: 0, y: 0 }, op, opId),
      );
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
    edges.push(
      createDefaultEdge(joinNamespaces(ROOT_NAMESPACE, START_ID), startNode.id),
    );
  }
  edges.push(...buildEdges(graph.nodes));

  return graph;
}

const validate = getSchema<Diagram>('Diagram');

export function loadTemplate(template: SectionTemplate): Graph {
  const stubDiagram = exportDiagram(new NodeManager([]), [], {});
  stubDiagram.ops = template.ops;
  const initialNodes: DiagramEditorNode[] = [];

  if (template.inputs) {
    if (Array.isArray(template.inputs)) {
      for (const input of template.inputs) {
        initialNodes.push(createSectionInputNode(input, input, { x: 0, y: 0 }));
      }
    } else {
      for (const [remappedId, targetId] of Object.entries(template.inputs)) {
        initialNodes.push(
          createSectionInputNode(remappedId, targetId, { x: 0, y: 0 }),
        );
      }
    }
  }

  if (template.buffers) {
    if (Array.isArray(template.buffers)) {
      for (const buffer of template.buffers) {
        initialNodes.push(
          createSectionBufferNode(buffer, buffer, { x: 0, y: 0 }),
        );
      }
    } else {
      for (const [remappedId, targetId] of Object.entries(template.buffers)) {
        initialNodes.push(
          createSectionBufferNode(remappedId, targetId, { x: 0, y: 0 }),
        );
      }
    }
  }

  if (template.outputs) {
    for (const output of template.outputs) {
      initialNodes.push(createSectionOutputNode(output, { x: 0, y: 0 }));
    }
  }

  return buildGraph(stubDiagram, { nodes: initialNodes, edges: [] });
}
