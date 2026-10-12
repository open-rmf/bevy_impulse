import {
  alpha,
  Box,
  Button,
  type ButtonProps,
  Paper,
  Stack,
  Typography,
} from '@mui/material';
import type { NodeProps } from '@xyflow/react';
import { type JSX, memo } from 'react';
import { useInteractionVisualization } from '../interaction-visualization-provider';
import { LAYOUT_OPTIONS } from '../utils/layout';

const CompactNodeSize = 42;

export interface BaseNodeProps extends NodeProps {
  color?: ButtonProps['color'];
  icon?: React.JSX.Element | string;
  label: string;
  caption?: string;
  handles?: JSX.Element;
  highlight?: boolean;
  compact?: boolean;
}

function BaseNode({
  color,
  icon: materialIconOrSymbol,
  label,
  caption,
  handles,
  selected,
  id,
  highlight,
  compact,
}: BaseNodeProps) {
  const { activeNodeIds, visitedNodeIds } = useInteractionVisualization();
  const interactionActive = activeNodeIds.has(id);
  const interactionVisited = visitedNodeIds.has(id) && !interactionActive;
  const icon =
    typeof materialIconOrSymbol === 'string' ? (
      <span className={`material-symbols-${materialIconOrSymbol}`} />
    ) : (
      materialIconOrSymbol
    );

  return (
    <Paper
      sx={(theme) => ({
        borderRadius: compact ? '50%' : undefined,
        outline: interactionActive
          ? `2px solid ${theme.palette.success.main}`
          : interactionVisited
            ? `2px solid ${alpha(theme.palette.info.main, 0.35)}`
            : highlight
              ? `2px solid ${theme.palette.warning.main}`
              : undefined,
        boxShadow: interactionActive
          ? [
              `0 0 0 4px ${alpha(theme.palette.success.main, 0.28)}`,
              `0 0 18px 6px ${alpha(theme.palette.success.main, 0.35)}`,
            ].join(', ')
          : interactionVisited
            ? [
                `0 0 0 2px ${alpha(theme.palette.info.main, 0.28)}`,
                `0 0 8px 3px ${alpha(theme.palette.info.main, 0.35)}`,
              ].join(', ')
            : undefined,
        transition: theme.transitions.create(['box-shadow', 'outline-color'], {
          duration: theme.transitions.duration.shortest,
        }),
      })}
    >
      <Button
        title={label}
        aria-label={compact ? label : undefined}
        color={color}
        fullWidth={!compact}
        startIcon={compact ? undefined : icon}
        variant={selected ? 'contained' : 'outlined'}
        sx={{
          textTransform: 'none',
          ...(compact
            ? {
                width: CompactNodeSize,
                minWidth: CompactNodeSize,
                height: CompactNodeSize,
                borderRadius: '50%',
                padding: 0,
              }
            : {
                width: LAYOUT_OPTIONS.nodeWidth,
                height: LAYOUT_OPTIONS.nodeHeight,
              }),
        }}
      >
        {compact ? (
          icon
        ) : (
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
        )}
      </Button>
      {handles}
    </Paper>
  );
}

export default memo(BaseNode);
