import { Card, CardContent, CardHeader, Stack, TextField } from '@mui/material';
import type { NodeReplaceChange } from '@xyflow/react';
import type { OperationNode } from '..';

export interface OpIdFormProps {
  node: OperationNode;
  onChange?: (change: NodeReplaceChange<OperationNode>) => void;
}

function OpIdForm({ node, onChange }: OpIdFormProps) {
  return (
    <Card>
      <CardHeader title="Edit Operation" />
      <CardContent>
        <Stack spacing={2}>
          <TextField
            required
            label="id"
            defaultValue={node.id}
            onChange={(ev) => {
              node.data.opId = ev.target.value;
              onChange?.({
                type: 'replace',
                id: node.id,
                item: { ...node },
              });
            }}
          />
        </Stack>
      </CardContent>
    </Card>
  );
}

export default OpIdForm;
