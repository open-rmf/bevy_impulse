import { TextField } from '@mui/material';
import type { EdgeChange } from '@xyflow/react';
import type { SplitKeyEdge, SplitRemainingEdge, SplitSeqEdge } from '../types';

export type SplitEdge = SplitKeyEdge | SplitSeqEdge | SplitRemainingEdge;

export interface SplitEdgeFormProps {
  edge: SplitEdge;
  onChanges?: (change: EdgeChange<SplitEdge>[]) => void;
}

function SplitEdgeForm({ edge, onChanges }: SplitEdgeFormProps) {
  const handleDataChange = (
    event: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>,
  ) => {
    if (edge.type === 'splitKey') {
      const newKey = event.target.value;
      onChanges?.([
        {
          type: 'replace',
          id: edge.id,
          item: { ...edge, data: { key: newKey } },
        },
      ]);
    } else if (edge.type === 'splitSeq') {
      const newSeq = Number.parseInt(event.target.value, 10);
      if (!Number.isNaN(newSeq)) {
        onChanges?.([
          {
            type: 'replace',
            id: edge.id,
            item: { ...edge, data: { seq: newSeq } },
          },
        ]);
      }
    }
  };

  return (
    <>
      {edge.type === 'splitKey' && (
        <TextField
          label="Key"
          value={edge.data?.key ?? ''}
          onChange={handleDataChange}
          fullWidth
        />
      )}
      {edge.type === 'splitSeq' && (
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

export default SplitEdgeForm;
