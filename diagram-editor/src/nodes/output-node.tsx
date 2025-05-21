import { Button, Paper } from '@mui/material';
import type { NodeProps } from '@xyflow/react';
import { Handle, Position } from '@xyflow/react';

import type { DiagramEditorNode } from '.';

export function OutputNode({
  id,
  isConnectable,
  selected,
  sourcePosition = Position.Bottom,
}: NodeProps<DiagramEditorNode>) {
  return (
    <Paper sx={{ minWidth: 100 }}>
      <Button
        fullWidth
        variant={selected ? 'contained' : 'outlined'}
        sx={{ textTransform: 'none' }}
      >
        {id}
      </Button>
      <Handle
        type="source"
        position={sourcePosition}
        isConnectable={isConnectable}
      />
    </Paper>
  );
}
