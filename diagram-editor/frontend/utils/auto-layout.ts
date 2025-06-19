import type { NodePositionChange } from '@xyflow/react';

import type { DiagramEditorEdge, DiagramEditorNode } from '..';

export interface AutoLayoutOptions {
  rootPosition: { x: number; y: number };
  cellWidth: number;
  cellHeight: number;
}

const DEFAULT_OPTIONS: AutoLayoutOptions = {
  rootPosition: { x: 0, y: 0 },
  cellWidth: 200,
  cellHeight: 100,
};

/**
 * Layout an array of diagram nodes so that they don't overlap.
 */
export function autoLayout(
  start: string,
  nodes: DiagramEditorNode[],
  edges: DiagramEditorEdge[],
  {
    cellWidth = DEFAULT_OPTIONS.cellWidth,
    cellHeight = DEFAULT_OPTIONS.cellHeight,
  }: Partial<AutoLayoutOptions> = DEFAULT_OPTIONS,
): NodePositionChange[] {
  interface WorkingData {
    node: DiagramEditorNode;
    nextIds: string[];
  }

  const map = new Map(
    nodes.map((node) => [node.id, { node, nextIds: [] } as WorkingData]),
  );

  const getWorkingData = (id: string) => {
    const workingData = map.get(id);
    if (!workingData) {
      throw new Error(`node ${id} not found`);
    }
    return workingData;
  };

  for (const edge of edges) {
    const source = getWorkingData(edge.source);
    source.nextIds.push(edge.target);
  }

  const getNode = (id: string) => getWorkingData(id).node;
  const getNextIds = (id: string) => getWorkingData(id).nextIds;

  const rootNode = getNode(start);
  const rootPosition = { ...rootNode.position };
  const changes: NodePositionChange[] = [];
  const fifo = [{ node: getNode(start), depth: 1 }];
  let maxX = rootNode.position.x;
  for (let ctx = fifo.shift(); ctx; ctx = fifo.shift()) {
    const { node, depth } = ctx;
    const nextNodeIds = getNextIds(node.id);
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
