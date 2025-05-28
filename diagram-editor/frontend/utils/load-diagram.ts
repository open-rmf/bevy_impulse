import Ajv from 'ajv';
import addFormats from 'ajv-formats';

import type { Edge } from '@xyflow/react';
import diagramSchema from '../diagram.schema.json';
import { type DiagramEditorNode, START_ID, TERMINATE_ID } from '../nodes';
import type {
  BufferSelection,
  Diagram,
  NamespacedOperation,
  NextOperation,
} from '../types/diagram';

export interface Graph {
  startNodeId: string;
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
  return graph;
}

export function getNodeId(next: NextOperation): string {
  if (typeof next === 'string') {
    return next;
  }
  if ('builtin' in next) {
    return `builtin:${next.builtin}`;
  }
  const [namespace, opId] = Object.entries(next)[0];
  return `${namespace}:${opId}`;
}

export function getBufferIds(buffer: BufferSelection): string[] {
  if (typeof buffer === 'string') {
    return [getNodeId(buffer)];
  }
  if (Array.isArray(buffer)) {
    return buffer.map((b) => getNodeId(b));
  }
  return [getNodeId(buffer as NamespacedOperation)];
}

export function getNextIds(
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
  const startNodeId = getNodeId(diagram.start);
  const edges: Edge[] = [
    {
      id: `${START_ID}->${startNodeId}`,
      source: START_ID,
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
  return { startNodeId, nodes, edges };
}

const ajv = new Ajv();
addFormats(ajv);
ajv.addFormat('uint', /^[0-9]+$/);
const validate = ajv.compile<Diagram>(diagramSchema);
