import { Button, Paper } from '@mui/material';
import type { NodeProps } from '@xyflow/react';
import { Handle, Position } from '@xyflow/react';

import type React from 'react';

export interface BaseNodeProps extends NodeProps {
  icon?: React.JSX.Element;
  label: string;
  variant: 'input' | 'output' | 'inputOutput';
}

function BaseNode({
  icon,
  label,
  variant,
  isConnectable,
  selected,
  sourcePosition = Position.Bottom,
  targetPosition = Position.Top,
}: BaseNodeProps) {
  return (
    <Paper sx={{ minWidth: 100 }}>
      {(variant === 'input' || variant === 'inputOutput') && (
        <Handle
          type="target"
          position={targetPosition}
          isConnectable={isConnectable}
        />
      )}
      <Button
        fullWidth
        startIcon={icon}
        variant={selected ? 'contained' : 'outlined'}
        sx={{ textTransform: 'none' }}
      >
        {label}
      </Button>
      {(variant === 'output' || variant === 'inputOutput') && (
        <Handle
          type="source"
          position={sourcePosition}
          isConnectable={isConnectable}
        />
      )}
    </Paper>
  );
}

export default BaseNode;
