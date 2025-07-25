import { Button, Paper } from '@mui/material';
import { Handle, type NodeProps, Position } from '@xyflow/react';
import type { BuiltinNode } from '.';

function TerminateNode({
  isConnectable,
  targetPosition = Position.Top,
  width,
  height,
}: NodeProps<BuiltinNode>) {
  return (
    <Paper>
      <Handle
        type="target"
        position={targetPosition}
        isConnectable={isConnectable}
      />
      <Button
        fullWidth
        variant="outlined"
        disabled
        sx={{
          width,
          height,
        }}
      >
        Terminate
      </Button>
    </Paper>
  );
}

export default TerminateNode;
