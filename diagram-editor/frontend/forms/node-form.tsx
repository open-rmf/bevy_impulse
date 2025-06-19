import { Card, CardContent, CardHeader, Stack, TextField } from '@mui/material';
import type { NodeReplaceChange } from '@xyflow/react';
import type { OperationNode } from '..';

export interface NodeFormProps {
  node: OperationNode<'node'>;
  onChange?: (change: NodeReplaceChange<OperationNode<'node'>>) => void;
}

function NodeForm({ node, onChange }: NodeFormProps) {
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
          <TextField
            required
            label="builder"
            defaultValue={node.data.builder}
            onChange={(ev) => {
              node.data.builder = ev.target.value;
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

export default NodeForm;
