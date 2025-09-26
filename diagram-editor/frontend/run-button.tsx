import {
  Button,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  Popover,
  Stack,
  TextField,
  Tooltip,
  Typography,
  useTheme,
} from '@mui/material';
import { useMemo, useRef, useState } from 'react';
import { useApiClient } from './api-client-provider';
import { useNodeManager } from './node-manager';
import { MaterialSymbol } from './nodes';
import { useRegistry } from './registry-provider';
import { useTemplates } from './templates-provider';
import { useEdges } from './use-edges';
import { exportDiagram } from './utils/export-diagram';

type ResponseContent = { raw: string } | { err: string };

export function RunButton() {
  const nodeManager = useNodeManager();
  const edges = useEdges();
  const [openPopover, setOpenPopover] = useState(false);
  const buttonRef = useRef<HTMLButtonElement>(null);
  const theme = useTheme();
  const [requestJson, setRequestJson] = useState('');
  const [responseContent, setResponseContent] = useState<ResponseContent>({
    raw: '',
  });
  const apiClient = useApiClient();
  const [templates, _setTemplates] = useTemplates();
  const registry = useRegistry();
  const [running, setRunning] = useState(false);

  const requestError = useMemo(() => {
    try {
      JSON.parse(requestJson);
      return false;
    } catch {
      return true;
    }
  }, [requestJson]);

  const responseError = useMemo(() => {
    return 'err' in responseContent;
  }, [responseContent]);

  const responseValue = useMemo(() => {
    if ('err' in responseContent) {
      return `Error: ${responseContent.err}`;
    } else {
      return responseContent.raw;
    }
  }, [responseContent]);

  const handleRunClick = () => {
    try {
      const request = JSON.parse(requestJson);
      const diagram = exportDiagram(registry, nodeManager, edges, templates);
      apiClient.postRunWorkflow(diagram, request).subscribe({
        next: (response) => {
          setResponseContent({ raw: JSON.stringify(response, null, 2) });
          setRunning(false);
        },
        error: (err) => {
          setResponseContent({ err: (err as Error).message });
          setRunning(false);
        },
      });
      setRunning(true);
    } catch (e) {
      setResponseContent({ err: (e as Error).message });
    }
  };

  return (
    <>
      <Tooltip title="Run Workflow">
        <Button ref={buttonRef} onClick={() => setOpenPopover(true)}>
          <MaterialSymbol symbol="play_arrow" />
        </Button>
      </Tooltip>
      <Popover
        open={openPopover}
        onClose={() => setOpenPopover(false)}
        anchorEl={buttonRef.current}
        anchorOrigin={{
          vertical: 'bottom',
          horizontal: 'center',
        }}
        transformOrigin={{
          vertical: 'top',
          horizontal: 'center',
        }}
        slotProps={{
          paper: {
            sx: {
              overflow: 'visible',
              mt: 0.5,
              backgroundColor: theme.palette.background.paper,
              border: `1px solid ${theme.palette.divider}`,
              '&:before': {
                content: '""',
                position: 'absolute',
                top: 0,
                left: '50%',
                transform: 'translateY(-50%) translateX(-50%) rotate(45deg)',
                width: 16,
                height: 16,
                backgroundColor: theme.palette.background.paper,
                backgroundImage: 'inherit',
                borderTop: `1px solid ${theme.palette.divider}`,
                borderLeft: `1px solid ${theme.palette.divider}`,
              },
            },
          },
        }}
      >
        <DialogTitle>Run Workflow</DialogTitle>
        <Divider />
        <DialogContent sx={{ width: 500 }}>
          <Stack spacing={2}>
            <Typography variant="body1">Request:</Typography>
            <TextField
              fullWidth
              multiline
              rows={10}
              variant="outlined"
              value={requestJson}
              slotProps={{
                htmlInput: {
                  sx: { fontFamily: 'monospace', whiteSpace: 'nowrap' },
                },
              }}
              onChange={(e) => setRequestJson(e.target.value)}
              error={requestError}
              sx={{ backgroundColor: theme.palette.background.paper }}
            />
            <Typography variant="body1">Response:</Typography>
            <TextField
              fullWidth
              multiline
              rows={10}
              variant="outlined"
              value={responseValue}
              slotProps={{
                htmlInput: {
                  sx: { fontFamily: 'monospace', whiteSpace: 'nowrap' },
                },
              }}
              error={responseError}
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button
            variant="contained"
            onClick={handleRunClick}
            loading={running}
            startIcon={<MaterialSymbol symbol="play_arrow" />}
          >
            Run
          </Button>
        </DialogActions>
      </Popover>
    </>
  );
}
