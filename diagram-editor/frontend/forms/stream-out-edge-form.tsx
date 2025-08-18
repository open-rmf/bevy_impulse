import { TextField } from '@mui/material';
import type { EdgeChange } from '@xyflow/react';
import type { StreamOutEdge } from '../edges';

export interface StreamOutEdgeFormProps {
  edge: StreamOutEdge;
  onChange?: (change: EdgeChange<StreamOutEdge>) => void;
}

export function StreamOutEdgeForm({ edge, onChange }: StreamOutEdgeFormProps) {
  const handleDataChange = (
    event: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>,
  ) => {
    const newStreamId = event.target.value;
    onChange?.({
      type: 'replace',
      id: edge.id,
      item: {
        ...edge,
        data: { ...edge.data, output: { streamId: newStreamId } },
      },
    });
  };

  return (
    <>
      <TextField
        label="Stream"
        value={edge.data.output.streamId}
        onChange={handleDataChange}
        fullWidth
      />
    </>
  );
}
