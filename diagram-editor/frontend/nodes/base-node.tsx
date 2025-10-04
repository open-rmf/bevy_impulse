import {
  Box,
  Button,
  type ButtonProps,
  Paper,
  Stack,
  Typography,
} from '@mui/material';
import type { NodeProps } from '@xyflow/react';
import { type JSX, memo } from 'react';
import { LAYOUT_OPTIONS } from '../utils/layout';

export interface BaseNodeProps extends NodeProps {
  color?: ButtonProps['color'];
  icon?: React.JSX.Element | string;
  label: string;
  caption?: string;
  handles?: JSX.Element;
}

function BaseNode({
  color,
  icon: materialIconOrSymbol,
  label,
  caption,
  handles,
  selected,
}: BaseNodeProps) {
  const icon =
    typeof materialIconOrSymbol === 'string' ? (
      <span className={`material-symbols-${materialIconOrSymbol}`} />
    ) : (
      materialIconOrSymbol
    );

  return (
    <Paper>
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
      {handles}
    </Paper>
  );
}

export default memo(BaseNode);
