import { Card, CardContent, CardHeader, TextField } from '@mui/material';
import type { EdgeReplaceChange } from '@xyflow/react';
import type { UnzipEdge } from '..';

export interface UnzipEdgeFormProps {
  edge: UnzipEdge;
  onChange: (change: EdgeReplaceChange<UnzipEdge>) => void;
}

function UnzipEdgeForm({ edge, onChange }: UnzipEdgeFormProps) {
  return (
    <Card>
      <CardHeader title="Edit Edge" />
      <CardContent>
        <TextField
          label="index"
          type="number"
          value={edge.data?.seq ?? 0}
          onChange={(ev) => {
            const value = Number.parseInt(ev.target.value, 10);
            if (!Number.isNaN(value) && edge.data) {
              edge.data.seq = value;
              onChange?.({
                type: 'replace',
                id: edge.id,
                item: edge,
              });
            }
          }}
        />
      </CardContent>
    </Card>
  );
}

export default UnzipEdgeForm;
