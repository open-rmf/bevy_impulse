import { Card, CardContent, CardHeader, Stack, TextField } from '@mui/material';
import type { OperationFormProps } from '.';

export type NodeFormProps = OperationFormProps;

function NodeForm({ node, onChange }: NodeFormProps) {
  if (node.data.type !== 'node') {
    throw new Error('expected node operation');
  }

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
                item: node,
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
                item: node,
              });
            }}
          />
        </Stack>
      </CardContent>
    </Card>
  );
}

export default NodeForm;
