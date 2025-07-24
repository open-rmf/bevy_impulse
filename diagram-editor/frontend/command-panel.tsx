import { Button, ButtonGroup, styled, Tooltip } from '@mui/material';
import { type NodeChange, Panel } from '@xyflow/react';
import React from 'react';
import AutoLayoutButton from './auto-layout-button';
import { MaterialSymbol } from './nodes/icons';
import type { DiagramEditorNode } from './types';
import EditTemplatesDialog from './edit-templates-dialog';
import { EditorMode, useEditorMode } from './editor-mode';

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
  const [openEditTemplatesDialog, setOpenEditTemplatesDialog] =
    React.useState(false);
  const [editorMode] = useEditorMode();

  return (
    <>
      <Panel position="top-center">
        <ButtonGroup variant="contained">
          {editorMode.mode === EditorMode.Normal && (
            <Tooltip title="Templates">
              <Button onClick={() => setOpenEditTemplatesDialog(true)}>
                <MaterialSymbol symbol="architecture" />
              </Button>
            </Tooltip>
          )}
          <AutoLayoutButton onNodeChanges={onNodeChanges} />
          {editorMode.mode === EditorMode.Normal && (
            <>
              <Tooltip title="Export Diagram">
                <Button onClick={onExportClick}>
                  <MaterialSymbol symbol="download" />
                </Button>
              </Tooltip>
              <Tooltip title="Load Diagram">
                {/* biome-ignore lint/a11y/useValidAriaRole: button used as a label, should have no role */}
                <Button component="label" role={undefined}>
                  <MaterialSymbol symbol="upload_file" />
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
            </>
          )}
        </ButtonGroup>
      </Panel>
      <EditTemplatesDialog
        open={openEditTemplatesDialog}
        onClose={() => setOpenEditTemplatesDialog(false)}
      />
    </>
  );
}

export default React.memo(CommandPanel);
