import {
  Button,
  ButtonGroup,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Stack,
  TextField,
  Typography,
} from '@mui/material';
import { deflateSync, strToU8 } from 'fflate';
import React from 'react';
import type { DiagramEditorEdge } from './edges';
import { NodeManager } from './node-manager';
import type { DiagramEditorNode } from './nodes';
import { MaterialSymbol } from './nodes/icons';
import { exportDiagram } from './utils/export-diagram';

export interface ExportDiagramDialogProps {
  open: boolean;
  onClose: () => void;
  nodes: DiagramEditorNode[];
  edges: DiagramEditorEdge[];
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

    const nodeManager = new NodeManager(nodes);
    const diagram = exportDiagram(nodeManager, edges);
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

  const [copiedShareLink, setCopiedShareLink] = React.useState(false);

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
          <ButtonGroup>
            <TextField
              variant="outlined"
              value={dialogData?.shareLink}
              fullWidth
              size="small"
            />
            <Button
              variant="contained"
              aria-label="copy share link"
              onClick={() => {
                if (!dialogData || copiedShareLink) {
                  return;
                }
                navigator.clipboard.writeText(dialogData.shareLink);
                setCopiedShareLink(true);
              }}
            >
              {copiedShareLink ? (
                <MaterialSymbol symbol="check" />
              ) : (
                <MaterialSymbol symbol="content_copy" />
              )}
            </Button>
          </ButtonGroup>
          <Stack direction="row" justifyContent="space-between">
            <Typography variant="h6">Export JSON</Typography>
            <Button
              variant="contained"
              onClick={handleDownload}
              startIcon={<MaterialSymbol symbol="download" />}
            >
              Download
            </Button>
          </Stack>
          <TextField
            multiline
            maxRows={20}
            variant="outlined"
            value={dialogData?.diagramJson}
            slotProps={{
              htmlInput: { style: { fontFamily: 'monospace' } },
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
