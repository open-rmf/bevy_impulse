import { Box, Button, type ButtonProps, Paper } from '@mui/material';
import { Handle, type NodeProps, Position } from '@xyflow/react';
import React from 'react';
import { LAYOUT_OPTIONS } from '../utils/layout';

export interface BaseNodeProps extends NodeProps {
  color?: ButtonProps['color'];
  icon?: React.JSX.Element | string;
  label: string;
  variant: 'input' | 'output' | 'inputOutput';
}

function BaseNode({
  color,
  icon: materialIconOrSymbol,
  label,
  variant,
  isConnectable,
  selected,
  sourcePosition = Position.Bottom,
  targetPosition = Position.Top,
}: BaseNodeProps) {
  const icon =
    typeof materialIconOrSymbol === 'string' ? (
      <span className={`material-symbols-${materialIconOrSymbol}`} />
    ) : (
      materialIconOrSymbol
    );
  return (
    <Paper>
      {(variant === 'input' || variant === 'inputOutput') && (
        <Handle
          type="target"
          position={targetPosition}
          isConnectable={isConnectable}
        />
      )}
      <Button
        title={label}
        color={color}
        fullWidth
        startIcon={icon}
        variant={selected ? 'contained' : 'outlined'}
        sx={{
          textTransform: 'none',
          width: LAYOUT_OPTIONS.nodeWidth,
          height: LAYOUT_OPTIONS.nodeHeight,
        }}
      >
        <Box
          component="span"
          sx={{
            minWidth: 0,
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
          }}
        >
          {label}
        </Box>
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

export default React.memo(BaseNode);
