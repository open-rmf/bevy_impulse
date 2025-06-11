import type { NodeProps } from '@xyflow/react';
import type { DiagramEditorNode } from '.';

import { Button, Paper } from '@mui/material';
import { Handle, Position } from '@xyflow/react';

export function TerminateNode({
  isConnectable,
  targetPosition = Position.Top,
}: NodeProps<DiagramEditorNode>) {
  return (
    <Paper>
      <Handle
        type="target"
        position={targetPosition}
        isConnectable={isConnectable}
      />
      <Button fullWidth variant="outlined" disabled>
        Terminate
      </Button>
    </Paper>
  );
}
