import type { NodePositionChange } from '@xyflow/react';

import type { DiagramEditorEdge, DiagramEditorNode } from '../types';

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

type Position = { x: number; y: number };

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
    position: Position;
    outEdges: DiagramEditorEdge[];
  }

  const workingSet = new Map(
    nodes.map((node) => [
      node.id,
      {
        node,
        position: { x: 0, y: 0 },
        outEdges: [],
      } as WorkingData,
    ]),
  );

  const getData = (id: string) => {
    const data = workingSet.get(id);
    if (!data) {
      throw new Error(`node ${id} not found`);
    }
    return data;
  };

  for (const edge of edges) {
    const data = getData(edge.source);
    data.outEdges.push(edge);
  }

  const fifo = [{ id: start, depth: 1 }];
  const visited = new Set<string>();

  for (let ctx = fifo.shift(); ctx; ctx = fifo.shift()) {
    const { id: parentId, depth } = ctx;
    if (visited.has(parentId)) {
      continue;
    }
    visited.add(parentId);

    const parentData = getData(parentId);
    const { position: parentPosition, outEdges } = parentData;
    let offsetX = -((outEdges.length - 1) * cellWidth) / 2;

    for (const edge of outEdges) {
      const { position } = getData(edge.target);
      position.x = parentPosition.x + position.x + offsetX;
      position.y = depth * cellHeight;
      offsetX += cellWidth;

      // if it is already queued, move it to the back of the queue
      const existing = fifo.findIndex((ctx) => ctx.id === edge.target);
      if (existing > -1) {
        fifo.splice(existing, 1);
      }
      fifo.push({ id: edge.target, depth: depth + 1 });
    }
  }

  // `node.position` contains the original position
  const startPosition = getData(start).node.position;
  const changes: NodePositionChange[] = [];
  // only make changes for visited node, meaning that unconnected nodes will not be affected.
  for (const id of visited.values()) {
    const { position } = getData(id);
    changes.push({
      id,
      type: 'position',
      position: {
        x: startPosition.x + position.x,
        y: startPosition.y + position.y,
      },
    });
  }
  return changes;
}
