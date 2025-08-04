import { TextField } from '@mui/material';
import type { EdgeChange } from '@xyflow/react';
import type { BufferKeyEdge, BufferSeqEdge } from '../edges';

export type BufferEdge = BufferKeyEdge | BufferSeqEdge;

export interface BufferEdgeFormProps {
  edge: BufferEdge;
  onChange?: (changes: EdgeChange<BufferEdge>) => void;
}

function BufferEdgeForm({ edge, onChange }: BufferEdgeFormProps) {
  const handleDataChange = (
    event: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>,
  ) => {
    if (edge.type === 'bufferKey') {
      const newKey = event.target.value;
      onChange?.({
        type: 'replace',
        id: edge.id,
        item: { ...edge, data: { key: newKey } },
      });
    } else if (edge.type === 'bufferSeq') {
      const newSeq = Number.parseInt(event.target.value, 10);
      if (!Number.isNaN(newSeq)) {
        onChange?.({
          type: 'replace',
          id: edge.id,
          item: { ...edge, data: { seq: newSeq } },
        });
      }
    }
  };

  return (
    <>
      {edge.type === 'bufferKey' && (
        <TextField
          label="Key"
          value={edge.data?.key ?? ''}
          onChange={handleDataChange}
          fullWidth
        />
      )}
      {edge.type === 'bufferSeq' && (
        <TextField
          label="Sequence"
          type="number"
          value={edge.data?.seq ?? 0}
          onChange={handleDataChange}
          fullWidth
        />
      )}
    </>
  );
}

export default BufferEdgeForm;
