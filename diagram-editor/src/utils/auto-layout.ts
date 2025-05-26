import type { NodePositionChange } from '@xyflow/react';

import type { DiagramEditorNode } from '../nodes';
import { getNextIds } from './load-diagram';

/**
 * Layout an array of diagram nodes so that they don't overlap.
 */
export function autoLayout(
  start: string,
  nodes: DiagramEditorNode[],
  rootPosition = { x: 0, y: 0 },
): NodePositionChange[] {
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

  const firstNode = getNode(start);
  const changes: NodePositionChange[] = [
    {
      id: firstNode.id,
      type: 'position',
      position: { x: rootPosition.x, y: rootPosition.y + cellHeight },
    },
  ];
  const fifo = [{ node: getNode(start), depth: 2 }];
  let maxX = firstNode.position.x;
  for (let ctx = fifo.shift(); ctx; ctx = fifo.shift()) {
    const { node, depth } = ctx;
    const nextNodeIds = getNextIds(node, nodes);
    let currentX = node.position.x - ((nextNodeIds.length - 1) * cellWidth) / 2;
    for (const nextNodeId of nextNodeIds) {
      const nextNode = getNode(nextNodeId);
      const position = { x: currentX, y: depth * cellHeight };
      // If it is not in the initial position, that means that the node has multiple parents,
      // in that case, move it to the center of its parents.
      if (nextNode.position.x !== rootPosition.x) {
        position.x = (nextNode.position.x + currentX) / 2;
      }
      currentX += cellWidth;
      if (currentX > maxX) {
        maxX = position.x;
      }

      changes.push({
        id: nextNode.id,
        type: 'position',
        position,
      });
      nextNode.position = position;

      const existing = fifo.findIndex((ctx) => ctx.node.id === nextNode.id);
      if (existing > -1) {
        fifo.splice(existing, 1);
      }
      fifo.push({ node: nextNode, depth: depth + 1 });
    }
  }

  return changes;
}
