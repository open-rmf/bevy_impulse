import DownloadIcon from '@mui/icons-material/Download';
import UploadIcon from '@mui/icons-material/UploadFile';
import { Button, ButtonGroup, styled, Tooltip } from '@mui/material';
import { type NodeChange, Panel } from '@xyflow/react';
import React from 'react';
import AutoLayoutButton from './auto-layout-button';
import type { DiagramEditorNode } from './types';

export interface CommandPanelProps {
  onNodeChanges: (changes: NodeChange<DiagramEditorNode>[]) => void;
  onExportClick: () => void;
  onLoadDiagram: (jsonStr: string) => void;
}

const VisuallyHiddenInput = styled('input')({
  clip: 'rect(0 0 0 0)',
  clipPath: 'inset(50%)',
  height: 1,
  overflow: 'hidden',
  position: 'absolute',
  bottom: 0,
  left: 0,
  whiteSpace: 'nowrap',
  width: 1,
});

function CommandPanel({
  onNodeChanges,
  onExportClick,
  onLoadDiagram,
}: CommandPanelProps) {
  return (
    <Panel position="top-center">
      <ButtonGroup variant="contained">
        <AutoLayoutButton onNodeChanges={onNodeChanges} />
        <Tooltip title="Export Diagram">
          <Button onClick={onExportClick}>
            <DownloadIcon />
          </Button>
        </Tooltip>
        <Tooltip title="Load Diagram">
          {/* biome-ignore lint/a11y/useValidAriaRole: button used as a label, should have no role */}
          <Button component="label" role={undefined}>
            <UploadIcon />
            <VisuallyHiddenInput
              type="file"
              accept="application/json"
              aria-label="load diagram"
              onChange={async (ev) => {
                if (ev.target.files) {
                  const json = await ev.target.files[0].text();
                  onLoadDiagram(json);
                }
              }}
              onClick={(ev) => {
                // Reset the input value so that the same file can be loaded multiple times
                (ev.target as HTMLInputElement).value = '';
              }}
            />
          </Button>
        </Tooltip>
      </ButtonGroup>
    </Panel>
  );
}

export default React.memo(CommandPanel);
