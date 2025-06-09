import {
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Stack,
  TextField,
  Typography,
} from '@mui/material';
import type { Edge } from '@xyflow/react';
import { deflateSync, strToU8 } from 'fflate';
import type { DiagramEditorNode } from './nodes';
import { exportDiagram } from './utils/export-diagram';
import React from 'react';

export interface ExportDiagramDialogProps {
  open: boolean;
  onClose: () => void;
  nodes: DiagramEditorNode[];
  edges: Edge[];
}

interface DialogData {
  shareLink: string;
  diagramJson: string;
}

function ExportDiagramDialog({
  open,
  onClose,
  nodes,
  edges,
}: ExportDiagramDialogProps) {
  const [dialogData, setDialogData] = React.useState<DialogData | null>(null);

  React.useLayoutEffect(() => {
    if (!open) {
      // To ensure that animations look correct, we need to keep the last value.
      // This is also why we cannot use `useMemo` as it must return a value.
      return;
    }

    const diagram = exportDiagram(nodes, edges);
    const diagramJsonMin = JSON.stringify(diagram);
    // Compress the JSON string to Uint8Array
    const compressedData = deflateSync(strToU8(diagramJsonMin));
    // Convert Uint8Array to a binary string for btoa
    let binaryString = '';
    for (let i = 0; i < compressedData.length; i++) {
      binaryString += String.fromCharCode(compressedData[i]);
    }
    const base64Diagram = btoa(binaryString);

    const shareLink = `${window.location.origin}?diagram=${encodeURIComponent(base64Diagram)}`;

    const diagramJsonPretty = JSON.stringify(diagram, null, 2);

    const dialogData = {
      shareLink,
      diagramJson: diagramJsonPretty,
    };

    setDialogData(dialogData);
  }, [open, nodes, edges]);

  const handleDownload = () => {
    if (!dialogData) {
      return;
    }

    const blob = new Blob([dialogData.diagramJson], {
      type: 'application/json',
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'diagram.json';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  return (
    <Dialog
      onClose={onClose}
      open={open}
      fullWidth
      maxWidth="md"
      keepMounted={false}
    >
      <DialogTitle>Export Diagram</DialogTitle>
      <DialogContent>
        <Stack spacing={2}>
          <Typography variant="h6">Share</Typography>
          <TextField variant="outlined" value={dialogData?.shareLink} />
          <Stack direction="row" justifyContent="space-between">
            <Typography variant="h6">Diagram JSON</Typography>
            <Button variant="contained" onClick={handleDownload}>
              Download
            </Button>
          </Stack>
          <TextField
            multiline
            maxRows={20}
            variant="outlined"
            value={dialogData?.diagramJson}
            slotProps={{
              input: { style: { fontFamily: 'monospace' } },
            }}
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Close</Button>
      </DialogActions>
    </Dialog>
  );
}

export default ExportDiagramDialog;
