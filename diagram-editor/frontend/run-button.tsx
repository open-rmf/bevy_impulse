import {
  Box,
  Button,
  Checkbox,
  Divider,
  FormControlLabel,
  Stack,
  TextField,
  Typography,
  useTheme,
} from '@mui/material';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { Subscription } from 'rxjs';
import { useApiClient } from './api-client-provider';
import { useDiagramProperties } from './diagram-properties-provider';
import { useInteractionVisualization } from './interaction-visualization-provider';
import { useNodeManager } from './node-manager';
import { MaterialSymbol } from './nodes';
import { useRegistry } from './registry-provider';
import { useTemplates } from './templates-provider';
import type { Diagram, DiagramOperation } from './types/api';
import { useEdges } from './use-edges';
import { exportDiagram } from './utils/export-diagram';

type ResponseContent = { raw: string } | { err: string };
type RunningMode = 'run' | null;

interface ExecutionTimelineEntry {
  seq: number;
  operationId: string;
}

const DefaultResponseContent: ResponseContent = { raw: '' };
const MaxExecutionTimelineEntries = 200;

function enableInteractionTraceForOps(ops: Record<string, DiagramOperation>) {
  for (const op of Object.values(ops)) {
    op.trace = 'on';
    if (op.type === 'scope') {
      enableInteractionTraceForOps(op.ops);
    }
  }
}

function enableInteractionTrace(diagram: Diagram): Diagram {
  const interactionDiagram = JSON.parse(JSON.stringify(diagram)) as Diagram;
  interactionDiagram.default_trace = 'on';
  enableInteractionTraceForOps(interactionDiagram.ops);
  return interactionDiagram;
}

export interface RunPanelProps {
  requestJsonString: string;
  onRequestJsonStringChange: (requestJsonString: string) => void;
}

export function RunPanel({
  requestJsonString,
  onRequestJsonStringChange,
}: RunPanelProps) {
  const nodeManager = useNodeManager();
  const edges = useEdges();
  const theme = useTheme();
  const [responseContent, setResponseContent] = useState<ResponseContent>(
    DefaultResponseContent,
  );
  const apiClient = useApiClient();
  const {
    clearInteractionVisualization,
    markInteractionFinished,
    markInteractionOperationFinished,
    markInteractionOperationStarted,
    setWorkflowRunning,
  } = useInteractionVisualization();
  const [templates] = useTemplates();
  const registry = useRegistry();
  const [runningMode, setRunningMode] = useState<RunningMode>(null);
  const [showProgress, setShowProgress] = useState(true);
  const showProgressRef = useRef(showProgress);
  const [executionTimeline, setExecutionTimeline] = useState<
    ExecutionTimelineEntry[]
  >([]);
  const interactionEventCounter = useRef(0);
  const interactionSessionRef = useRef<Awaited<
    ReturnType<NonNullable<typeof apiClient.wsInteractWithWorkflow>>
  > | null>(null);
  const interactionSubscriptionRef = useRef<Subscription | null>(null);
  const [diagramProperties] = useDiagramProperties();

  useEffect(() => {
    setWorkflowRunning(runningMode !== null);
    return () => setWorkflowRunning(false);
  }, [runningMode, setWorkflowRunning]);

  const closeInteractionSession = useCallback(() => {
    interactionSubscriptionRef.current?.unsubscribe();
    interactionSubscriptionRef.current = null;
    interactionSessionRef.current?.close();
    interactionSessionRef.current = null;
  }, []);

  useEffect(() => {
    return closeInteractionSession;
  }, [closeInteractionSession]);

  useEffect(() => {
    showProgressRef.current = showProgress;
    if (!showProgress) {
      clearInteractionVisualization();
      setExecutionTimeline([]);
    }
  }, [clearInteractionVisualization, showProgress]);

  const requestError = useMemo(() => {
    try {
      JSON.parse(requestJsonString);
      return false;
    } catch {
      return true;
    }
  }, [requestJsonString]);

  const responseError = useMemo(() => {
    return 'err' in responseContent;
  }, [responseContent]);

  const responseValue = useMemo(() => {
    if ('err' in responseContent) {
      return `Error: ${responseContent.err}`;
    }
    return responseContent.raw;
  }, [responseContent]);

  const handleRequestJsonChange = (value: string) => {
    onRequestJsonStringChange(value);
  };

  const runWithPost = (diagram: Diagram, request: unknown) => {
    apiClient.postRunWorkflow(diagram, request).subscribe({
      next: (response) => {
        setResponseContent({ raw: JSON.stringify(response, null, 2) });
        setRunningMode(null);
      },
      error: (err) => {
        setResponseContent({ err: (err as Error).message });
        setRunningMode(null);
      },
    });
  };

  const handleRunClick = async () => {
    closeInteractionSession();
    clearInteractionVisualization();
    setExecutionTimeline([]);
    interactionEventCounter.current = 0;

    try {
      const request = JSON.parse(requestJsonString);
      const diagram = exportDiagram(
        registry,
        nodeManager,
        edges,
        templates,
        diagramProperties,
      );
      setResponseContent(DefaultResponseContent);
      setRunningMode('run');

      if (!showProgress || !apiClient.wsInteractWithWorkflow) {
        runWithPost(diagram, request);
        return;
      }

      const interactionSession = await apiClient.wsInteractWithWorkflow(
        enableInteractionTrace(diagram),
        request,
      );
      interactionSessionRef.current = interactionSession;
      interactionSubscriptionRef.current =
        interactionSession.interactionMessages$.subscribe({
          next: (msg) => {
            if (
              msg.type === 'feedback' &&
              'operationStarted' in msg &&
              typeof msg.operationStarted === 'string'
            ) {
              if (!showProgressRef.current) {
                return;
              }
              const operationId = msg.operationStarted;
              markInteractionOperationStarted(operationId);
              const entry = {
                seq: ++interactionEventCounter.current,
                operationId,
              };
              setExecutionTimeline((prev) =>
                [...prev, entry].slice(-MaxExecutionTimelineEntries),
              );
              return;
            }

            if (
              msg.type === 'feedback' &&
              'operationFinished' in msg &&
              typeof msg.operationFinished === 'string'
            ) {
              if (!showProgressRef.current) {
                return;
              }
              markInteractionOperationFinished(msg.operationFinished);
              return;
            }

            if (msg.type === 'finish') {
              markInteractionFinished();
              if ('ok' in msg) {
                setResponseContent({ raw: JSON.stringify(msg.ok, null, 2) });
              } else {
                setResponseContent({ err: msg.err });
              }
              setRunningMode(null);
              closeInteractionSession();
            }
          },
          error: (err) => {
            markInteractionFinished();
            setResponseContent({ err: (err as Error).message });
            setRunningMode(null);
            closeInteractionSession();
          },
          complete: () => {
            setRunningMode(null);
          },
        });
    } catch (e) {
      setResponseContent({ err: (e as Error).message });
      setRunningMode(null);
      closeInteractionSession();
    }
  };

  return (
    <Stack
      spacing={2}
      sx={{
        minHeight: 0,
        overflowY: 'auto',
        p: 2,
      }}
    >
      <Stack spacing={1}>
        <Typography variant="h6">Run Workflow</Typography>
        <Typography variant="body2" color="text.secondary">
          Request
        </Typography>
        <TextField
          fullWidth
          multiline
          minRows={6}
          maxRows={12}
          variant="outlined"
          value={requestJsonString}
          slotProps={{
            htmlInput: {
              sx: { fontFamily: 'monospace', whiteSpace: 'nowrap' },
            },
          }}
          onChange={(e) => handleRequestJsonChange(e.target.value)}
          error={requestError}
          sx={{ backgroundColor: theme.palette.background.paper }}
        />
      </Stack>
      <Stack direction="row" spacing={1}>
        <Button
          variant="contained"
          onClick={handleRunClick}
          disabled={runningMode !== null}
          loading={runningMode === 'run'}
          startIcon={<MaterialSymbol symbol="play_arrow" />}
        >
          Run
        </Button>
        <FormControlLabel
          control={
            <Checkbox
              checked={showProgress}
              onChange={(event) => setShowProgress(event.target.checked)}
            />
          }
          label="Show progress"
        />
      </Stack>
      <Divider />
      <Stack spacing={1}>
        <Stack direction="row" spacing={1} sx={{ alignItems: 'center' }}>
          <Typography variant="body1">Response</Typography>
          {'err' in responseContent ? (
            <MaterialSymbol
              symbol="error"
              sx={{ color: theme.palette.error.main }}
            />
          ) : 'raw' in responseContent && responseContent.raw.length > 0 ? (
            <MaterialSymbol
              symbol="check_circle"
              sx={{ color: theme.palette.success.main }}
            />
          ) : null}
        </Stack>
        <TextField
          fullWidth
          multiline
          minRows={6}
          maxRows={12}
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
      {showProgress && executionTimeline.length > 0 && (
        <Stack spacing={1}>
          <Typography variant="body1">Execution timeline</Typography>
          <Box
            sx={{
              border: `1px solid ${theme.palette.divider}`,
              borderRadius: 1,
              maxHeight: 220,
              overflowY: 'auto',
              px: 1,
              py: 0.5,
            }}
          >
            {executionTimeline.map((entry) => (
              <Stack
                key={entry.seq}
                direction="row"
                spacing={1}
                sx={{ alignItems: 'baseline' }}
              >
                <Typography variant="caption" color="text.secondary">
                  {entry.seq}
                </Typography>
                <Typography
                  variant="body2"
                  sx={{
                    fontFamily: 'monospace',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap',
                  }}
                >
                  {entry.operationId}
                </Typography>
              </Stack>
            ))}
          </Box>
        </Stack>
      )}
    </Stack>
  );
}
