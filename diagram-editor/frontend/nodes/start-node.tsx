import { Button, Paper } from '@mui/material';
import type { NodeProps } from '@xyflow/react';
import { Handle, Position } from '@xyflow/react';

import type { DiagramEditorNode } from '..';

function StartNode({
  isConnectable,
  sourcePosition = Position.Bottom,
}: NodeProps<DiagramEditorNode>) {
  return (
    <Paper sx={{ minWidth: 100 }}>
      <Button fullWidth variant="outlined" disabled>
        Start
      </Button>
      <Handle
        type="source"
        position={sourcePosition}
        isConnectable={isConnectable}
      />
    </Paper>
  );
}

export default StartNode;
