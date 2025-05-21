import Ajv from 'ajv';
import addFormats from 'ajv-formats';

import type { Edge } from '@xyflow/react';
import diagramSchema from '../diagram.schema.json';
import type { DiagramEditorNode } from '../nodes';
import type {
  BufferSelection,
  Diagram,
  NamespacedOperation,
  NextOperation,
} from '../types/diagram';

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

function getBufferIds(buffer: BufferSelection): string[] {
  if (typeof buffer === 'string') {
    return [getNodeId(buffer)];
  }
  if (Array.isArray(buffer)) {
    return buffer.map((b) => getNodeId(b));
  }
  return [getNodeId(buffer as NamespacedOperation)];
}

function getNextIds(
  node: DiagramEditorNode,
  allNodes: DiagramEditorNode[],
): string[] {
  switch (node.data.type) {
    case 'buffer': {
      const nextIds: string[] = [];
      for (const joinNode of allNodes) {
        if (
          joinNode.data.type === 'join' ||
          joinNode.data.type === 'serialized_join'
        ) {
          const buffers = getBufferIds(joinNode.data.buffers);
          if (buffers.includes(node.id)) {
            nextIds.push(joinNode.id);
          }
        }
      }
      return nextIds;
    }
    case 'buffer_access':
    case 'join':
    case 'listen':
    case 'node':
    case 'serialized_join':
    case 'transform': {
      return [getNodeId(node.data.next)];
    }
    case 'fork_clone':
    case 'unzip': {
      return node.data.next.map((next) => getNodeId(next));
    }
    case 'fork_result': {
      return [getNodeId(node.data.ok), getNodeId(node.data.err)];
    }
    case 'split': {
      const nextIds: string[] = [];
      if (node.data.keyed) {
        nextIds.push(
          ...Object.values(node.data.keyed).map((next) => getNodeId(next)),
        );
      }
      if (node.data.sequential) {
        nextIds.push(
          ...Object.values(node.data.sequential).map((next) => getNodeId(next)),
        );
      }
      if (node.data.remaining) {
        nextIds.push(getNodeId(node.data.remaining));
      }
      return nextIds;
    }
    case 'section': {
      if (node.data.connect) {
        return Object.values(node.data.connect).map((next) => getNodeId(next));
      }
      return [];
    }
    default: {
      return [];
    }
  }
}

function buildGraph(diagram: Diagram): Graph {
  const nodes: DiagramEditorNode[] = [
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
      ([opId, op]) =>
        ({
          id: opId,
          type: 'inputOutput',
          position: { x: 0, y: 0 },
          data: op,
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
    for (const nextId of getNextIds(node, nodes)) {
      edges.push({
        id: `${node.id}->${nextId}`,
        source: node.id,
        target: nextId,
      });
    }
  }
  return { nodes, edges };
}

/**
 * Layout an array of diagram nodes so that they don't overlap.
 */
function autoLayout(
  start: NextOperation,
  nodes: DiagramEditorNode[],
  rootPosition = { x: 0, y: 0 },
) {
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
  firstNode.position = { x: rootPosition.x, y: rootPosition.y + cellHeight };
  const fifo = [{ node: getNode(getNodeId(start)), depth: 2 }];
  let maxX = firstNode.position.x;
  for (let ctx = fifo.shift(); ctx; ctx = fifo.shift()) {
    const { node, depth } = ctx;
    const nextNodeIds = getNextIds(node, nodes);
    let x = node.position.x - ((nextNodeIds.length - 1) * cellWidth) / 2;
    for (const nextNodeId of nextNodeIds) {
      const nextNode = getNode(nextNodeId);
      nextNode.position.y = depth * cellHeight;
      // If the node is in the inital position, move it directly below the parent node.
      // If it is not in the initial position, that means that the node has multiple parents,
      // in that case, move it to the center of its parents.
      if (nextNode.position.x === rootPosition.x) {
        nextNode.position.x = x;
      } else {
        nextNode.position.x = (nextNode.position.x + x) / 2;
      }
      x += cellWidth;
      if (x > maxX) {
        maxX = nextNode.position.x;
      }

      const existing = fifo.findIndex((ctx) => ctx.node.id === nextNode.id);
      if (existing > -1) {
        fifo.splice(existing, 1);
      }
      fifo.push({ node: nextNode, depth: depth + 1 });
    }
  }
}
