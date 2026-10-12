import type { PopoverPosition } from '@mui/material';
import { useEffect, useMemo, useState } from 'react';
import { useDiagramSidePanel } from './diagram-side-panel-controller';
import {
  constrainEditPopoverPosition,
  getDiagramSidePanelWidth,
} from './diagram-side-panel-layout';

function useViewportWidth(): number {
  const [viewportWidth, setViewportWidth] = useState(() =>
    typeof window === 'undefined' ? 0 : window.innerWidth,
  );

  useEffect(() => {
    const updateViewportWidth = () => setViewportWidth(window.innerWidth);
    window.addEventListener('resize', updateViewportWidth);
    return () => window.removeEventListener('resize', updateViewportWidth);
  }, []);

  return viewportWidth;
}

export function useDiagramSidePanelWidth(): number {
  const {
    state: { open, expanded },
  } = useDiagramSidePanel();
  const viewportWidth = useViewportWidth();

  return useMemo(
    () => getDiagramSidePanelWidth(viewportWidth, { open, expanded }),
    [expanded, open, viewportWidth],
  );
}

export function useResponsiveEditPopoverPosition(
  anchorPosition?: PopoverPosition,
): PopoverPosition | undefined {
  const {
    state: { open, expanded },
  } = useDiagramSidePanel();
  const viewportWidth = useViewportWidth();

  return useMemo(
    () =>
      anchorPosition
        ? constrainEditPopoverPosition({
            anchorPosition,
            viewportWidth,
            sidePanel: { open, expanded },
          })
        : undefined,
    [anchorPosition, expanded, open, viewportWidth],
  );
}
