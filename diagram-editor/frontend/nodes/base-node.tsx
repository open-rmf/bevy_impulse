import {
  Box,
  Button,
  type ButtonProps,
  Paper,
  Stack,
  Typography,
} from '@mui/material';
import { type NodeProps, Position } from '@xyflow/react';
import { memo } from 'react';
import { Handle, type HandleProps, HandleType } from '../handles';
import { LAYOUT_OPTIONS } from '../utils/layout';

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
  extraHandles?: HandleProps[];
}

function BaseNode({
  color,
  icon: materialIconOrSymbol,
  label,
  variant,
  inputHandleType = HandleType.Data,
  outputHandleType = HandleType.Data,
  caption,
  extraHandles,
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
          variant={inputHandleType}
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
          variant={outputHandleType}
        />
      )}
      {extraHandles?.map((handleProps, i) => (
        <Handle key={i.toString()} {...handleProps} />
      ))}
    </Paper>
  );
}

export default memo(BaseNode);
