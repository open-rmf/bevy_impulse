import Ajv from 'ajv';
import addFormats from 'ajv-formats';

import type { Edge } from '@xyflow/react';
import diagramSchema from '../diagram.schema.json';
import type { DiagramEditorNode } from '../nodes';
import type { Diagram, NextOperation } from '../types/diagram';

const ajv = new Ajv();
addFormats(ajv);
ajv.addFormat('uint', /^[0-9]+$/);
const validate = ajv.compile<Diagram>(diagramSchema);

export interface Graph {
  nodes: DiagramEditorNode[];
  edges: Edge[];
}

export function loadDiagramJson(jsonStr: string): Graph {
  const diagram = JSON.parse(jsonStr);
  const valid = validate(diagram);
  if (!valid) {
    throw validate.errors;
  }

  const graph = buildGraph(diagram);
  autoLayout(diagram.start, graph.nodes);
  return graph;
}

function buildGraph(diagram: Diagram): Graph {
  const nodes = [
    {
      id: 'builtin:start',
      type: 'start',
      position: { x: 0, y: 0 },
      selectable: false,
      data: {},
    },
    {
      id: 'builtin:terminate',
      type: 'terminate',
      position: { x: 0, y: 0 },
      selectable: false,
      data: {},
    },
    ...Object.entries(diagram.ops).map(
      ([id, data]) =>
        ({
          id,
          type: 'default',
          position: { x: 0, y: 0 },
          data: {
            // TODO: this is a placeholder for reactflow default nodes, it should be removed when all
            // diagram operations have custom nodes implemented.
            label: id,
            ...data,
          },
        }) satisfies DiagramEditorNode,
    ),
  ];
  const startNodeId = getNodeId(diagram.start);
  const edges: Edge[] = [
    {
      id: `builtin:start->${startNodeId}`,
      source: 'builtin:start',
      target: startNodeId,
    },
  ];
  for (const node of nodes) {
    for (const nextId of getNextIds(node)) {
      edges.push({
        id: `${node.id}->${nextId}`,
        source: node.id,
        target: nextId,
      });
    }
  }
  return { nodes, edges };
}

function getNextIds(node: DiagramEditorNode): string[] {
  switch (node.data.type) {
    case 'node': {
      return [getNodeId(node.data.next)];
    }
    case 'fork_clone': {
      return node.data.next.map((next) => getNodeId(next));
    }
    default: {
      return [];
    }
  }
}

function getNodeId(next: NextOperation): string {
  if (typeof next === 'string') {
    return next;
  }
  if ('builtin' in next) {
    return `builtin:${next.builtin}`;
  }
  const [namespace, opId] = Object.entries(next)[0];
  return `${namespace}:${opId}`;
}

/**
 * Layout an array of diagram nodes so that they don't overlap.
 */
function autoLayout(start: NextOperation, nodes: DiagramEditorNode[]) {
  const cellWidth = 200;
  const cellHeight = 100;
  const map = new Map(nodes.map((node) => [node.id, node]));

  const getNode = (id: string) => {
    const node = map.get(id);
    if (!node) {
      throw new Error(`node ${id} not found`);
    }
    return node;
  };

  const firstNode = getNode(getNodeId(start));
  firstNode.position = { x: 0, y: cellHeight };
  const stack = [{ node: getNode(getNodeId(start)), depth: 2 }];
  for (let ctx = stack.pop(); ctx; ctx = stack.pop()) {
    const { node, depth } = ctx;
    const nextNodeIds = getNextIds(node);
    let x = node.position.x - ((nextNodeIds.length - 1) * cellWidth) / 2;
    for (const nextNodeId of nextNodeIds) {
      const nextNode = getNode(nextNodeId);
      nextNode.position.y = depth * cellHeight;
      if (nextNode.position.x === 0) {
        nextNode.position.x = x;
      } else {
        nextNode.position.x = (nextNode.position.x + x) / 2;
      }
      x += cellWidth;
      stack.push({ node: nextNode, depth: depth + 1 });
    }
  }
}
