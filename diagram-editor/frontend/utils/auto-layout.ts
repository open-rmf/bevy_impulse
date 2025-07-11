import type { NodeChange } from '@xyflow/react';
import * as dagre from 'dagre';
import { NodeManager } from '../node-manager';
import { START_ID, TERMINATE_ID } from '../nodes';
import type {
  DiagramEditorEdge,
  DiagramEditorNode,
  OperationNode,
} from '../types';
import { joinNamespaces } from './namespace';

export interface AutoLayoutOptions {
  rootPosition: { x: number; y: number };
  cellWidth: number;
  cellHeight: number;
}

const DEFAULT_OPTIONS: AutoLayoutOptions = {
  rootPosition: { x: 0, y: 0 },
  cellWidth: 200,
  cellHeight: 50,
};

function isScopeNode(node: DiagramEditorNode): node is OperationNode<'scope'> {
  return node.type === 'scope';
}

export function autoLayout(
  nodes: DiagramEditorNode[],
  edges: DiagramEditorEdge[],
): NodeChange<DiagramEditorNode>[] {
  const nodeManager = new NodeManager(nodes);
  const dagreGraph = new dagre.graphlib.Graph().setDefaultEdgeLabel(() => ({}));
  dagreGraph.setGraph({ rankdir: 'TB' });

  const scopeChildrens: Record<string, DiagramEditorNode[]> = {};
  for (const node of nodes) {
    const parentNode = node.parentId
      ? nodeManager.getNode(node.parentId)
      : null;
    if (parentNode?.id) {
      if (!scopeChildrens[parentNode.id]) {
        scopeChildrens[parentNode.id] = [];
      }
      scopeChildrens[parentNode.id].push(node);
    }

    // exclude scope node from auto layout
    if (!isScopeNode(node)) {
      dagreGraph.setNode(node.id, {
        width: DEFAULT_OPTIONS.cellWidth,
        height: DEFAULT_OPTIONS.cellHeight,
      });
    }
  }

  for (const edge of edges) {
    const sourceNode = nodeManager.getNode(edge.source);
    const targetNode = nodeManager.getNode(edge.target);

    if (targetNode.type === 'scope') {
      dagreGraph.setEdge(
        edge.source,
        nodeManager.getNodeFromNamespaceOpId(
          joinNamespaces(targetNode.data.namespace, targetNode.data.opId),
          START_ID,
        ).id,
        { minlen: 2 },
      );
    } else if (sourceNode.type === 'scope') {
      dagreGraph.setEdge(
        nodeManager.getNodeFromNamespaceOpId(
          joinNamespaces(sourceNode.data.namespace, sourceNode.data.opId),
          TERMINATE_ID,
        ).id,
        edge.target,
        { minlen: 2 },
      );
    } else {
      dagreGraph.setEdge(edge.source, edge.target);
    }
  }

  dagre.layout(dagreGraph);

  const scopePositions: Record<string, { x: number; y: number }> = {};
  for (const [scopeNodeId, children] of Object.entries(scopeChildrens)) {
    // place the scope node in the middle of it's children.
    const positionSum = children.reduce(
      (prev, curr) => {
        prev.x += dagreGraph.node(curr.id).x;
        prev.y += dagreGraph.node(curr.id).y;
        return prev;
      },
      { x: 0, y: 0 },
    );
    scopePositions[scopeNodeId] = {
      x: positionSum.x / children.length,
      y: positionSum.y / children.length,
    };
  }

  const changes: NodeChange<DiagramEditorNode>[] = [];
  for (const node of nodes) {
    const nodePosition = dagreGraph.node(node.id);
    if (isScopeNode(node)) {
      continue;
    }

    if (node.parentId) {
      changes.push({
        type: 'position',
        id: node.id,
        position: {
          x: nodePosition.x - scopePositions[node.parentId].x + 300,
          y: nodePosition.y - scopePositions[node.parentId].y + 200,
        },
      });
    } else {
      changes.push({
        type: 'position',
        id: node.id,
        position: {
          x: nodePosition.x,
          y: nodePosition.y,
        },
      });
    }
  }
  for (const [scopeNodeId, position] of Object.entries(scopePositions)) {
    changes.push({
      type: 'position',
      id: scopeNodeId,
      position,
    });
    changes.push({
      type: 'dimensions',
      id: scopeNodeId,
      dimensions: {
        // TODO: calculate width and height based on children
        width: 600,
        height: 400,
      },
    });
  }

  return changes;
}
