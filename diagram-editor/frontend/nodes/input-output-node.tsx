import { Button, Paper } from '@mui/material';
import type { NodeProps } from '@xyflow/react';
import { Handle, Position } from '@xyflow/react';

import type { AnyOperationNode, DiagramEditorNode } from '.';
import { getIcon } from './icons';

function isOperationData(
  data: DiagramEditorNode['data'],
): data is AnyOperationNode['data'] {
  return 'type' in data;
}

export function InputOutputNode({
  data,
  isConnectable,
  selected,
  sourcePosition = Position.Bottom,
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
      <Handle
        type="source"
        position={sourcePosition}
        isConnectable={isConnectable}
      />
    </Paper>
  );
}
