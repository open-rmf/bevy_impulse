import type { Rect, XYPosition } from '@xyflow/react';

export interface LayoutOptions {
  nodeWidth: number;
  nodeHeight: number;
  scopePadding: {
    leftRight: number;
    topBottom: number;
  };
}

export const LAYOUT_OPTIONS: LayoutOptions = {
  nodeWidth: 200,
  nodeHeight: 50,
  scopePadding: {
    leftRight: 75,
    topBottom: 50,
  },
};

export function calculateScopeBounds(childrenPosition: XYPosition[]): Rect {
  if (childrenPosition.length === 0) {
    return {
      x: 0,
      y: 0,
      width: LAYOUT_OPTIONS.scopePadding.leftRight * 2,
      height: LAYOUT_OPTIONS.scopePadding.topBottom * 2,
    };
  }
  const minX = Math.min(...childrenPosition.map((n) => n.x));
  const maxX = Math.max(
    ...childrenPosition.map((n) => n.x + LAYOUT_OPTIONS.nodeWidth),
  );
  const minY = Math.min(...childrenPosition.map((n) => n.y));
  const maxY = Math.max(
    ...childrenPosition.map((n) => n.y + LAYOUT_OPTIONS.nodeHeight),
  );

  return {
    x: minX - LAYOUT_OPTIONS.scopePadding.leftRight,
    y: minY - LAYOUT_OPTIONS.scopePadding.topBottom,
    width: maxX - minX + LAYOUT_OPTIONS.scopePadding.leftRight * 2,
    height: maxY - minY + LAYOUT_OPTIONS.scopePadding.topBottom * 2,
  };
}
