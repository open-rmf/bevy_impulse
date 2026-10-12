import { Paper, Stack, Typography } from '@mui/material';
import { Panel, useConnection } from '@xyflow/react';
import {
  useCompatibilityChecker,
  useConnectionCompatibility,
  useDraggedConnectionCompatibility,
} from './connection-compatibility-provider';
import { useInteractionVisualization } from './interaction-visualization-provider';
import type { NodeManager } from './node-manager';
import type { CompatibilityResult } from './types/api';
import { useEdges } from './use-edges';
import { validateSourceOutputCapacity } from './utils/connection';

export interface ConnectionHintPanelProps {
  nodeManager: NodeManager;
  reconnectingEdgeId?: string;
  edgeResult?: CompatibilityResult;
}

export function ConnectionHintPanel({
  nodeManager,
  reconnectingEdgeId,
  edgeResult,
}: ConnectionHintPanelProps) {
  const connection = useConnection();
  const edges = useEdges();
  const dragResult = useDraggedConnectionCompatibility({
    otherNodeId: connection.toHandle?.nodeId,
    otherHandleId: connection.toHandle?.id,
    otherHandleType: connection.toHandle?.type ?? 'target',
  });
  const { menuPreview } = useCompatibilityChecker();
  const menuResult = useConnectionCompatibility(
    menuPreview?.connection ?? null,
    menuPreview?.nodeChanges,
  );
  const { isWorkflowRunning } = useInteractionVisualization();

  if (
    isWorkflowRunning ||
    (!connection.inProgress && !menuPreview && !edgeResult)
  ) {
    return null;
  }

  const sourceNode =
    connection.fromHandle &&
    nodeManager.tryGetNode(connection.fromHandle.nodeId);
  const sourceOutputCapacity =
    sourceNode && connection.fromHandle?.type === 'source'
      ? validateSourceOutputCapacity(
          sourceNode,
          connection.fromHandle.id,
          edges,
          reconnectingEdgeId,
        )
      : { valid: true as const };

  let message = !sourceOutputCapacity.valid
    ? sourceOutputCapacity.error
    : reconnectingEdgeId
      ? 'Drop on a port, or release on empty space to keep the original connection.'
      : connection.fromHandle?.type === 'target'
        ? 'Drop on an output, or release on empty space to add a previous operation.'
        : 'Drop on an input, or release on empty space to add a next operation.';
  let tone: 'info' | 'success' | 'error' = sourceOutputCapacity.valid
    ? 'info'
    : 'error';

  const previewing = connection.inProgress || !!menuPreview;
  const compatibility = connection.inProgress
    ? dragResult
    : menuPreview
      ? menuResult
      : edgeResult;
  if (
    (connection.inProgress && connection.toHandle) ||
    menuPreview ||
    edgeResult
  ) {
    message = compatibility?.reason ?? 'Checking compatibility…';
    tone =
      compatibility?.status === 'incompatible'
        ? 'error'
        : compatibility?.status === 'compatible'
          ? 'success'
          : 'info';
    if (
      previewing &&
      compatibility?.status === 'incompatible' &&
      !('blocked' in compatibility && compatibility.blocked)
    ) {
      message += ' You can still create this connection.';
    }
  }

  return (
    <Panel position="top-left">
      <Paper
        elevation={3}
        sx={{
          px: 2,
          py: 1.5,
          width: 320,
          border: 1,
          borderColor:
            tone === 'success'
              ? 'success.main'
              : tone === 'error'
                ? 'error.main'
                : 'divider',
        }}
      >
        <Stack spacing={0.5}>
          <Typography variant="subtitle2">Connection Helper</Typography>
          <Typography variant="body2">{message}</Typography>
          {compatibility?.sourceType && (
            <Typography variant="caption" color="text.secondary">
              Source: {compatibility.sourceType}
            </Typography>
          )}
          {compatibility?.targetType && (
            <Typography variant="caption" color="text.secondary">
              Target: {compatibility.targetType}
            </Typography>
          )}
        </Stack>
      </Paper>
    </Panel>
  );
}
