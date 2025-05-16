import type { NodeProps } from '@xyflow/react';
import type { DiagramEditorNode } from '.';

import { Handle, Position } from '@xyflow/react';
import { Button, Paper } from '@mui/material';

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
