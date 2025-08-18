import type { NodeChange, Rect } from '@xyflow/react';
import * as dagre from 'dagre';
import type { DiagramEditorEdge } from '../edges';
import { NodeManager } from '../node-manager';
import {
  type DiagramEditorNode,
  type OperationNode,
  START_ID,
  TERMINATE_ID,
} from '../nodes';
import { calculateScopeBounds, type LayoutOptions } from './layout';
import { joinNamespaces } from './namespace';

function isScopeNode(node: DiagramEditorNode): node is OperationNode<'scope'> {
  return node.type === 'scope';
}

export function autoLayout(
  nodes: DiagramEditorNode[],
  edges: DiagramEditorEdge[],
  options: LayoutOptions,
): NodeChange<DiagramEditorNode>[] {
  const nodeManager = new NodeManager(nodes);
  const dagreGraph = new dagre.graphlib.Graph().setDefaultEdgeLabel(() => ({}));
  dagreGraph.setGraph({
    rankdir: 'TB',
    ranksep: options.nodeHeight,
  });

  const scopeChildrens: Record<string, DiagramEditorNode[]> = {};
  for (const node of nodes) {
    const parentNode = node.parentId
      ? nodeManager.tryGetNode(node.parentId)
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
        // dagre requires the node dimensions to be known, the easy solution is to only use
        // fixed size nodes. The complex alternative is to delay auto layout until ReactFlow
        // finishes measuring the nodes, this is complex because
        //   1. There is no hook for when ReactFlow finishes measurements
        //   2. ReactFlow does not delay updating the DOM until the measurements are complete.
        //     2.1. This means that when loading a new diagram, there will be a short period where the layout is not yet computed.
        //   3. Measurements of a node may change multiple times before it "stabilizes".
        //     3.1. This means that even if all nodes have measurements, they may not be final measurements.
        //          If the measurements change after the layout is computed, the layout will be wrong.
        width: options.nodeWidth,
        height: options.nodeHeight,
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

  const scopeBounds: Record<string, Rect> = {};
  for (const [scopeNodeId, children] of Object.entries(scopeChildrens)) {
    scopeBounds[scopeNodeId] = calculateScopeBounds(
      children.map((n) => dagreGraph.node(n.id)),
    );
  }

  const changes: NodeChange<DiagramEditorNode>[] = [];
  for (const node of nodes) {
    const nodePosition = dagreGraph.node(node.id);
    if (isScopeNode(node)) {
      continue;
    }

    if (node.parentId) {
      const scope = scopeBounds[node.parentId];
      changes.push({
        type: 'position',
        id: node.id,
        position: {
          x:
            nodePosition.x -
            // convert position to be relative to scope
            scope.x,
          y:
            nodePosition.y -
            // convert position to be relative to scope
            scope.y,
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
  for (const [scopeNodeId, bounds] of Object.entries(scopeBounds)) {
    changes.push({
      type: 'position',
      id: scopeNodeId,
      position: {
        x: bounds.x,
        y: bounds.y,
      },
    });
    changes.push({
      type: 'dimensions',
      id: scopeNodeId,
      dimensions: {
        width: bounds.width,
        height: bounds.height,
      },
      setAttributes: true,
    });
  }

  return changes;
}
