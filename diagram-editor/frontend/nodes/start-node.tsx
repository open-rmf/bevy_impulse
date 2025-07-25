import { Button, Paper } from '@mui/material';
import type { NodeProps } from '@xyflow/react';
import { Handle, Position } from '@xyflow/react';
import type { BuiltinNode } from '.';

function StartNode({
  isConnectable,
  sourcePosition = Position.Bottom,
  width,
  height,
}: NodeProps<BuiltinNode>) {
  return (
    <Paper sx={{ minWidth: 100 }}>
      <Button
        fullWidth
        variant="outlined"
        disabled
        sx={{
          width,
          height,
        }}
      >
        Start
      </Button>
      <Handle
        type="source"
        position={sourcePosition}
        isConnectable={isConnectable}
      />
    </Paper>
  );
}

export default StartNode;
