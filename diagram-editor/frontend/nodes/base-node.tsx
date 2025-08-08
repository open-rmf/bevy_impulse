import {
  Box,
  Button,
  type ButtonProps,
  Paper,
  Stack,
  Typography,
} from '@mui/material';
import { Handle, type NodeProps, Position } from '@xyflow/react';
import { memo, useCallback } from 'react';
import { exhaustiveCheck } from '../utils/exhaustive-check';
import { LAYOUT_OPTIONS } from '../utils/layout';

export enum HandleType {
  Data,
  Buffer,
  Stream,
  DataBuffer,
  DataStream,
}

export interface BaseNodeProps extends NodeProps {
  color?: ButtonProps['color'];
  icon?: React.JSX.Element | string;
  label: string;
  variant: 'input' | 'output' | 'inputOutput';
  /**
   * defaults to `HandleType.Data`.
   */
  inputHandleType?: HandleType;
  /**
   * defaults to `HandleType.Data`.
   */
  outputHandleType?: HandleType;
  caption?: string;
}

function BaseNode({
  color,
  icon: materialIconOrSymbol,
  label,
  variant,
  inputHandleType = HandleType.Data,
  outputHandleType = HandleType.Data,
  caption,
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

  const handleClassName = useCallback((handleType?: HandleType) => {
    if (handleType === undefined) {
      return undefined;
    }

    switch (handleType) {
      case HandleType.Data: {
        // use the default style
        return undefined;
      }
      case HandleType.Buffer: {
        return 'handle-buffer';
      }
      case HandleType.Stream: {
        return 'handle-stream';
      }
      case HandleType.DataBuffer: {
        return 'handle-data-buffer';
      }
      case HandleType.DataStream: {
        return 'handle-data-stream';
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
        <Stack>
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
          {caption && (
            <Typography
              variant="caption"
              fontSize={8}
              sx={{
                minWidth: 0,
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
              }}
            >
              {caption}
            </Typography>
          )}
        </Stack>
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
