import { Button, Paper } from '@mui/material';
import type { NodeProps } from '@xyflow/react';
import { Position } from '@xyflow/react';
import { Handle, HandleType } from '../handles';
import { LAYOUT_OPTIONS } from '../utils/layout';
import type { BuiltinNode } from '.';

function StartNodeComp({
  isConnectable,
  sourcePosition = Position.Bottom,
}: NodeProps<BuiltinNode>) {
  return (
    <Paper sx={{ minWidth: 100 }}>
      <Button
        fullWidth
        variant="outlined"
        disabled
        sx={{
          width: LAYOUT_OPTIONS.nodeWidth,
          height: LAYOUT_OPTIONS.nodeHeight,
        }}
      >
        Start
      </Button>
      <Handle
        type="source"
        position={sourcePosition}
        isConnectable={isConnectable}
        variant={HandleType.Data}
      />
    </Paper>
  );
}

export default StartNodeComp;
