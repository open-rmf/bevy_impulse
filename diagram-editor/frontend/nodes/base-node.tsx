import { Box, Button, type ButtonProps, Paper } from '@mui/material';
import { Handle, type NodeProps, Position } from '@xyflow/react';
import { memo, useCallback } from 'react';
import { EdgeCategory } from '../edges';
import { exhaustiveCheck } from '../utils/exhaustive-check';
import { LAYOUT_OPTIONS } from '../utils/layout';

export interface BaseNodeProps extends NodeProps {
  color?: ButtonProps['color'];
  icon?: React.JSX.Element | string;
  label: string;
  variant: 'input' | 'output' | 'inputOutput';
  /**
   * defaults to `EdgeCateogry.Data`.
   */
  inputHandleType?: EdgeCategory;
  /**
   * defaults to `EdgeCateogry.Data`.
   */
  outputHandleType?: EdgeCategory;
}

function BaseNode({
  color,
  icon: materialIconOrSymbol,
  label,
  variant,
  inputHandleType = EdgeCategory.Data,
  outputHandleType = EdgeCategory.Data,
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

  const handleClassName = useCallback((handleType?: EdgeCategory) => {
    if (handleType === undefined) {
      return undefined;
    }

    switch (handleType) {
      case EdgeCategory.Data: {
        // use the default style
        return undefined;
      }
      case EdgeCategory.Buffer: {
        return 'handle-buffer';
      }
      case EdgeCategory.Stream: {
        return undefined;
      }
      default: {
        exhaustiveCheck(handleType);
        throw new Error('unknown edge category');
      }
    }
  }, []);

  return (
    <Paper>
      {(variant === 'input' || variant === 'inputOutput') && (
        <Handle
          type="target"
          position={targetPosition}
          isConnectable={isConnectable}
          className={handleClassName(inputHandleType)}
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
          className={handleClassName(outputHandleType)}
        />
      )}
    </Paper>
  );
}

export default memo(BaseNode);
