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
  cellWidth: number;
  cellHeight: number;
  scopePadding: {
    leftRight: number;
    topBottom: number;
  };
}

const DEFAULT_OPTIONS: AutoLayoutOptions = {
  cellWidth: 200,
  cellHeight: 50,
  scopePadding: {
    leftRight: 100,
    topBottom: 50,
  },
};

function isScopeNode(node: DiagramEditorNode): node is OperationNode<'scope'> {
  return node.type === 'scope';
}

export function autoLayout(
  nodes: DiagramEditorNode[],
  edges: DiagramEditorEdge[],
  options?: Partial<AutoLayoutOptions>,
): NodeChange<DiagramEditorNode>[] {
  const mergedOptions = { ...DEFAULT_OPTIONS, ...options };
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
        width: mergedOptions.cellWidth,
        height: mergedOptions.cellHeight,
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

  interface ScopeState {
    x: number;
    y: number;
    width: number;
    height: number;
  }
  const scopePositions: Record<string, ScopeState> = {};
  for (const [scopeNodeId, children] of Object.entries(scopeChildrens)) {
    let sumX = 0;
    let sumY = 0;
    const firstChild = dagreGraph.node(children[0].id);
    let minX = firstChild.x;
    let maxX = firstChild.x;
    let minY = firstChild.y;
    let maxY = firstChild.y;

    for (const node of children) {
      const nodePosition = dagreGraph.node(node.id);
      sumX += nodePosition.x;
      sumY += nodePosition.y;
      minX = Math.min(minX, nodePosition.x);
      maxX = Math.max(maxX, nodePosition.x);
      minY = Math.min(minY, nodePosition.y);
      maxY = Math.max(maxY, nodePosition.y);
    }

    scopePositions[scopeNodeId] = {
      // place the scope node in the middle of it's children.
      x: sumX / children.length,
      y: sumY / children.length,
      width: maxX - minX + mergedOptions.scopePadding.leftRight * 2,
      height: maxY - minY + mergedOptions.scopePadding.topBottom * 2,
    };
  }

  const changes: NodeChange<DiagramEditorNode>[] = [];
  for (const node of nodes) {
    const nodePosition = dagreGraph.node(node.id);
    if (isScopeNode(node)) {
      continue;
    }

    if (node.parentId) {
      const scopePosition = scopePositions[node.parentId];
      changes.push({
        type: 'position',
        id: node.id,
        position: {
          x:
            nodePosition.x -
            // convert position relative to parent
            scopePosition.x +
            scopePosition.width / 2,
          y:
            nodePosition.y -
            // convert position relative to parent
            scopePosition.y +
            scopePosition.height / 2,
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
      position: {
        x: position.x,
        y: position.y,
      },
    });
    changes.push({
      type: 'dimensions',
      id: scopeNodeId,
      dimensions: {
        width: position.width,
        height: position.height,
      },
    });
  }

  return changes;
}
