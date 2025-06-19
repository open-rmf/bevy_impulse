import {
  Card,
  CardContent,
  CardHeader,
  FormControl,
  InputLabel,
  MenuItem,
  Select,
} from '@mui/material';
import type { EdgeReplaceChange } from '@xyflow/react';
import type { ForkResultErrEdge, ForkResultOkEdge } from '../types';

export type ForkResultEdge = ForkResultOkEdge | ForkResultErrEdge;

export interface ForkResultEdgeFormProps {
  edge: ForkResultEdge;
  onChange: (change: EdgeReplaceChange<ForkResultEdge>) => void;
}

function ForkResultEdgeForm({ edge, onChange }: ForkResultEdgeFormProps) {
  return (
    <Card>
      <CardHeader title="Edit Edge" />
      <CardContent>
        <FormControl fullWidth>
          <InputLabel id="fork-result-edge-type-label">Type</InputLabel>
          <Select
            labelId="fork-result-edge-type-label"
            label="Type"
            value={edge.type}
            onChange={(ev) => {
              const newType = ev.target.value;
              if (edge.type !== newType) {
                const newEdge = { ...edge, type: newType };
                onChange?.({
                  type: 'replace',
                  id: edge.id,
                  item: newEdge,
                });
              }
            }}
          >
            <MenuItem value="forkResultOk">Ok</MenuItem>
            <MenuItem value="forkResultErr">Error</MenuItem>
          </Select>
        </FormControl>
      </CardContent>
    </Card>
  );
}

export default ForkResultEdgeForm;
