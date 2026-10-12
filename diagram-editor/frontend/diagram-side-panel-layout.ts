import type { PopoverPosition } from '@mui/material';

export interface SidePanelLayoutState {
  open: boolean;
  expanded: boolean;
}

export const NormalSidePanelWidth = 420;
export const ExpandedSidePanelWidth = 900;
export const MinimumVisibleCanvasWidth = 56;
export const EditPopoverWidth = 420;
export const EditPopoverMargin = 16;
export const EditPopoverSidePanelGap = 16;

export function getDiagramSidePanelWidth(
  viewportWidth: number,
  sidePanel: SidePanelLayoutState,
): number {
  if (!sidePanel.open) {
    return 0;
  }

  return Math.min(
    sidePanel.expanded ? ExpandedSidePanelWidth : NormalSidePanelWidth,
    Math.max(0, viewportWidth - MinimumVisibleCanvasWidth),
  );
}

/** Right edge of the area the edit popover may occupy. */
function getEditPopoverRightBoundary(
  viewportWidth: number,
  sidePanel: SidePanelLayoutState,
): number {
  const sidePanelWidth = getDiagramSidePanelWidth(viewportWidth, sidePanel);
  return (
    viewportWidth -
    sidePanelWidth -
    (sidePanelWidth > 0 ? EditPopoverSidePanelGap : EditPopoverMargin)
  );
}

export function constrainEditPopoverPosition({
  anchorPosition,
  viewportWidth,
  sidePanel,
}: {
  anchorPosition: PopoverPosition;
  viewportWidth: number;
  sidePanel: SidePanelLayoutState;
}): PopoverPosition {
  const maximumLeft =
    getEditPopoverRightBoundary(viewportWidth, sidePanel) - EditPopoverWidth;

  return {
    left: Math.max(
      EditPopoverMargin,
      Math.min(anchorPosition.left, maximumLeft),
    ),
    top: anchorPosition.top,
  };
}

/** Places the edit popover beside a node, flipping to its left when it would not fit. */
export function getEditPopoverPositionForNode({
  nodeRect,
  viewportWidth,
  sidePanel,
}: {
  nodeRect: Pick<DOMRect, 'left' | 'right' | 'top'>;
  viewportWidth: number;
  sidePanel: SidePanelLayoutState;
}): PopoverPosition {
  const rightOfNode = nodeRect.right + EditPopoverMargin;
  const fitsRight =
    rightOfNode + EditPopoverWidth <=
    getEditPopoverRightBoundary(viewportWidth, sidePanel);

  return {
    left: fitsRight
      ? rightOfNode
      : nodeRect.left - EditPopoverWidth - EditPopoverMargin,
    top: nodeRect.top,
  };
}
