import { Button, Paper } from '@mui/material';
import { Handle, type NodeProps, Position } from '@xyflow/react';
import { LAYOUT_OPTIONS } from '../utils/layout';
import type { BuiltinNode } from '.';

function TerminateNodeComp({
  isConnectable,
  targetPosition = Position.Top,
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
          width: LAYOUT_OPTIONS.nodeWidth,
          height: LAYOUT_OPTIONS.nodeHeight,
        }}
      >
        Terminate
      </Button>
    </Paper>
  );
}

export default TerminateNodeComp;
