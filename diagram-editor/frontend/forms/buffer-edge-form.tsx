import {
  FormControl,
  InputLabel,
  MenuItem,
  Select,
  TextField,
} from '@mui/material';
import type { EdgeChange } from '@xyflow/react';
import { useId } from 'react';
import type { BufferEdge } from '../edges';

export interface BufferEdgeInputFormProps {
  edge: BufferEdge;
  onChange?: (changes: EdgeChange<BufferEdge>) => void;
}

export function BufferEdgeInputForm({
  edge,
  onChange,
}: BufferEdgeInputFormProps) {
  // TODO: check if target is a section and add support for connection section buffer slots.

  const handleDataChange = (
    event: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>,
  ) => {
    if (edge.data.input?.type === 'bufferKey') {
      const newKey = event.target.value;
      onChange?.({
        type: 'replace',
        id: edge.id,
        item: {
          ...edge,
          data: { ...edge.data, input: { type: 'bufferKey', key: newKey } },
        },
      });
    } else {
      const newSeq = Number.parseInt(event.target.value, 10);
      if (!Number.isNaN(newSeq)) {
        onChange?.({
          type: 'replace',
          id: edge.id,
          item: {
            ...edge,
            data: { ...edge.data, input: { type: 'bufferSeq', seq: newSeq } },
          },
        });
      }
    }
  };

  const labelId = useId();

  return (
    <>
      <FormControl>
        <InputLabel id={labelId}>Slot</InputLabel>
        <Select
          labelId={labelId}
          label="Slot"
          value={edge.data.input?.type || 'bufferSeq'}
        >
          <MenuItem value="bufferSeq">Index</MenuItem>
          <MenuItem value="bufferKey">Key</MenuItem>
        </Select>
      </FormControl>
      {edge.data.input === undefined ||
        (edge.data.input?.type === 'bufferSeq' && (
          <TextField
            label="Index"
            type="number"
            value={edge.data.input?.seq ?? 0}
            onChange={handleDataChange}
            fullWidth
          />
        ))}
      {edge.data.input?.type === 'bufferKey' && (
        <TextField
          label="Key"
          value={edge.data.input?.key ?? ''}
          onChange={handleDataChange}
          fullWidth
        />
      )}
    </>
  );
}
