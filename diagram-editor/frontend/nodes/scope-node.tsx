import { Box, useTheme } from '@mui/material';
import { type NodeProps, Position } from '@xyflow/react';
import { Handle, HandleType } from '../handles';
import type { OperationNode } from '.';

function ScopeNodeComp({
  isConnectable,
  sourcePosition = Position.Bottom,
  targetPosition = Position.Top,
  width,
  height,
}: NodeProps<OperationNode<'scope'>>) {
  const theme = useTheme();

  return (
    <>
      <Handle
        type="target"
        position={targetPosition}
        isConnectable={isConnectable}
        variant={HandleType.Data}
      />
      <Box
        sx={{
          opacity: 0.2,
          backgroundColor: theme.palette.secondary.main,
          borderRadius: 4,
          width,
          height,
          cursor: 'pointer',
        }}
      ></Box>
      <Handle
        type="source"
        position={sourcePosition}
        isConnectable={isConnectable}
        variant={HandleType.Data}
      />
      <Handle
        type="source"
        position={Position.Right}
        variant={HandleType.Stream}
      />
    </>
  );
}

export default ScopeNodeComp;
