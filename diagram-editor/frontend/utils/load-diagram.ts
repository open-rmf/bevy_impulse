import { v4 as uuidv4 } from 'uuid';
import type { DiagramEditorEdge } from '../edges';
import { NodeManager } from '../node-manager';
import {
  type DiagramEditorNode,
  isOperationNode,
  START_ID,
  TERMINATE_ID,
} from '../nodes';
import type { Diagram, DiagramOperation, SectionTemplate } from '../types/api';
import { getSchema } from './ajv';
import { exportDiagram } from './export-diagram';
import { LAYOUT_OPTIONS } from './layout';
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

const validate = getSchema<Diagram>('Diagram');

export function loadTemplate(template: SectionTemplate): Graph {
  const stubDiagram = exportDiagram(new NodeManager([]), [], {});
  stubDiagram.ops = template.ops;
  const graph = buildGraph(stubDiagram);

  // filter out builtin START and TERMINATE.
  const nodes = graph.nodes.filter((node) => {
    !node.parentId && node.type in ['start', 'terminate'];
  });

  if (template.inputs) {
    if (Array.isArray(template.inputs)) {
      for (const input of template.inputs) {
        nodes.push({
          id: uuidv4(),
          type: 'sectionInput',
          position: { x: 0, y: 0 },
          data: {
            namespace: ROOT_NAMESPACE,
            remappedId: input,
            targetId: input,
          },
          width: LAYOUT_OPTIONS.nodeWidth,
          height: LAYOUT_OPTIONS.nodeHeight,
        });
      }
    } else {
      for (const [remappedId, targetId] of Object.entries(template.inputs)) {
        nodes.push({
          id: uuidv4(),
          type: 'sectionInput',
          position: { x: 0, y: 0 },
          data: {
            namespace: ROOT_NAMESPACE,
            remappedId,
            targetId,
          },
          width: LAYOUT_OPTIONS.nodeWidth,
          height: LAYOUT_OPTIONS.nodeHeight,
        });
      }
    }
  }

  if (template.buffers) {
    if (Array.isArray(template.buffers)) {
      for (const buffer of template.buffers) {
        nodes.push({
          id: uuidv4(),
          type: 'sectionBuffer',
          position: { x: 0, y: 0 },
          data: {
            namespace: ROOT_NAMESPACE,
            remappedId: buffer,
            targetId: buffer,
          },
          width: LAYOUT_OPTIONS.nodeWidth,
          height: LAYOUT_OPTIONS.nodeHeight,
        });
      }
    } else {
      for (const [remappedId, targetId] of Object.entries(template.buffers)) {
        nodes.push({
          id: uuidv4(),
          type: 'sectionBuffer',
          position: { x: 0, y: 0 },
          data: {
            namespace: ROOT_NAMESPACE,
            remappedId,
            targetId,
          },
          width: LAYOUT_OPTIONS.nodeWidth,
          height: LAYOUT_OPTIONS.nodeHeight,
        });
      }
    }
  }

  if (template.outputs) {
    for (const output of template.outputs) {
      nodes.push({
        id: uuidv4(),
        type: 'sectionBuffer',
        position: { x: 0, y: 0 },
        data: {
          namespace: ROOT_NAMESPACE,
          remappedId: output,
          targetId: output,
        },
        width: LAYOUT_OPTIONS.nodeWidth,
        height: LAYOUT_OPTIONS.nodeHeight,
      });
    }
  }

  return { nodes, edges: graph.edges };
}
