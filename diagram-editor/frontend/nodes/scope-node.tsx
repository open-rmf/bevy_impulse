import { Box, useTheme } from '@mui/material';
import { Handle, type NodeProps, Position } from '@xyflow/react';
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
      />
      <Box
        sx={{
          opacity: 0.2,
          backgroundColor: theme.palette.secondary.main,
          borderRadius: 4,
          borderColor: 'red',
          width,
          height,
          cursor: 'pointer',
        }}
      ></Box>
      <Handle
        type="source"
        position={sourcePosition}
        isConnectable={isConnectable}
      />
    </>
  );
}

export default ScopeNodeComp;
