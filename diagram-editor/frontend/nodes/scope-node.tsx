import { Box, useTheme } from '@mui/material';
import { Handle, type NodeProps, Position } from '@xyflow/react';
import type { OperationNode } from '../types';

const PADDING = 150;

function ScopeNode({
  isConnectable,
  sourcePosition = Position.Bottom,
  targetPosition = Position.Top,
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
          backgroundColor: theme.palette.secondary.main,
          opacity: 0.2,
          width: 300,
          height: 300,
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

export default ScopeNode;
