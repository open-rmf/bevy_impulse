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
import type { SplitKeyEdge, SplitRemainingEdge, SplitSeqEdge } from '../types';

export type SplitEdge = SplitKeyEdge | SplitSeqEdge | SplitRemainingEdge;

export interface SplitEdgeFormProps {
  edge: SplitEdge;
  onChange: (change: EdgeReplaceChange<SplitEdge>) => void;
}

function SplitEdgeForm({ edge, onChange }: SplitEdgeFormProps) {
  const handleTypeChange = (event: SelectChangeEvent<SplitEdge['type']>) => {
    const newType = event.target.value as SplitEdge['type'];
    if (edge.type !== newType) {
      let newEdge: SplitEdge;
      if (newType === 'splitKey') {
        newEdge = {
          ...edge,
          type: newType,
          data: { key: '' },
        };
      } else if (newType === 'splitSeq') {
        newEdge = {
          ...edge,
          type: newType,
          data: { seq: 0 },
        };
      } else {
        newEdge = {
          ...edge,
          type: newType,
          data: {},
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
    if (edge.type === 'splitKey') {
      const newKey = event.target.value;
      onChange({
        type: 'replace',
        id: edge.id,
        item: { ...edge, data: { key: newKey } },
      });
    } else if (edge.type === 'splitSeq') {
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
            <InputLabel id="split-edge-type-label">Type</InputLabel>
            <Select
              labelId="split-edge-type-label"
              label="Type"
              value={edge.type}
              onChange={handleTypeChange}
            >
              <MenuItem value="splitKey">Key</MenuItem>
              <MenuItem value="splitSeq">Sequence</MenuItem>
              <MenuItem value="splitRemaining">Remaining</MenuItem>
            </Select>
          </FormControl>

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
        </Stack>
      </CardContent>
    </Card>
  );
}

export default SplitEdgeForm;
