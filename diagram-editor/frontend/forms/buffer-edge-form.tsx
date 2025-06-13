import {
  Card,
  CardContent,
  CardHeader,
  FormControl,
  InputLabel,
  MenuItem,
  Select,
  type SelectChangeEvent,
  Stack,
  TextField,
} from '@mui/material';
import type { EdgeReplaceChange } from '@xyflow/react';
import type { BufferKeyEdge, BufferSeqEdge } from '../edges';

export type BufferEdge = BufferKeyEdge | BufferSeqEdge;

export interface BufferEdgeFormProps {
  edge: BufferEdge;
  onChange: (change: EdgeReplaceChange<BufferEdge>) => void;
}

function BufferEdgeForm({ edge, onChange }: BufferEdgeFormProps) {
  const handleTypeChange = (event: SelectChangeEvent<BufferEdge['type']>) => {
    const newType = event.target.value as BufferEdge['type'];
    if (edge.type !== newType) {
      let newEdge: BufferEdge;
      if (newType === 'bufferKey') {
        newEdge = {
          ...edge,
          type: newType,
          data: { key: '' },
        };
      } else {
        newEdge = {
          ...edge,
          type: newType,
          data: { seq: 0 },
        };
      }
      onChange({
        type: 'replace',
        id: edge.id,
        item: newEdge,
      });
    }
  };

  const handleDataChange = (
    event: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>,
  ) => {
    if (edge.type === 'bufferKey') {
      const newKey = event.target.value;
      onChange({
        type: 'replace',
        id: edge.id,
        item: { ...edge, data: { key: newKey } },
      });
    } else if (edge.type === 'bufferSeq') {
      const newSeq = Number.parseInt(event.target.value, 10);
      if (!Number.isNaN(newSeq)) {
        onChange({
          type: 'replace',
          id: edge.id,
          item: { ...edge, data: { seq: newSeq } },
        });
      }
    }
  };

  return (
    <Card>
      <CardHeader title="Edit Edge" />
      <CardContent>
        <Stack spacing={2}>
          <FormControl fullWidth>
            <InputLabel id="buffer-edge-type-label">Type</InputLabel>
            <Select
              labelId="buffer-edge-type-label"
              label="Type"
              value={edge.type}
              onChange={handleTypeChange}
            >
              <MenuItem value="bufferKey">Key</MenuItem>
              <MenuItem value="bufferSeq">Sequence</MenuItem>
            </Select>
          </FormControl>

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
        </Stack>
      </CardContent>
    </Card>
  );
}

export default BufferEdgeForm;
