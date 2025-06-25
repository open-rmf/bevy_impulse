import { Button, Paper } from '@mui/material';
import type { NodeProps } from '@xyflow/react';
import { Handle, Position } from '@xyflow/react';
import type { DiagramEditorNode } from '../types';

function TerminateNode({
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

export default TerminateNode;
