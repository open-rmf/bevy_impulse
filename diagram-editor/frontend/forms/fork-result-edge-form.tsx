import { Card, CardContent, CardHeader, MenuItem, Select } from '@mui/material';
import type { EdgeReplaceChange } from '@xyflow/react';
import type { ForkResultErrEdge, ForkResultOkEdge } from '../edges';

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
        <Select
          label="type"
          value={edge.type}
          onChange={(ev) => {
            const newType = ev.target.value as 'forkResultOk' | 'forkResultErr';
            if (edge.type !== newType) {
              edge.type = newType;
              onChange?.({
                type: 'replace',
                id: edge.id,
                item: edge,
              });
            }
          }}
        >
          <MenuItem value="forkResultOk">Ok</MenuItem>
          <MenuItem value="forkResultErr">Error</MenuItem>
        </Select>
      </CardContent>
    </Card>
  );
}

export default ForkResultEdgeForm;
