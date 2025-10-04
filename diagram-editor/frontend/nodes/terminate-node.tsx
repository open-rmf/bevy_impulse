import { Button, Paper } from '@mui/material';
import { type NodeProps, Position } from '@xyflow/react';
import { Handle, HandleType } from '../handles';
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
        variant={HandleType.Data}
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
