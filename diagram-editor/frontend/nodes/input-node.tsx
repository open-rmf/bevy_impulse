import { Button, Paper } from '@mui/material';
import type { NodeProps } from '@xyflow/react';
import { Handle, Position } from '@xyflow/react';

import type { DiagramEditorNode } from '.';
import { getIcon } from './icons';
import { isOperationData } from './utils';

export function InputNode({
  data,
  isConnectable,
  selected,
  targetPosition = Position.Top,
}: NodeProps<DiagramEditorNode>) {
  const IconComponent = isOperationData(data) ? getIcon(data) : null;

  return (
    <Paper sx={{ minWidth: 100 }}>
      <Handle
        type="target"
        position={targetPosition}
        isConnectable={isConnectable}
      />
      <Button
        fullWidth
        startIcon={IconComponent ? <IconComponent /> : undefined}
        variant={selected ? 'contained' : 'outlined'}
        sx={{ textTransform: 'none' }}
      >
        {data.opId}
      </Button>
    </Paper>
  );
}
