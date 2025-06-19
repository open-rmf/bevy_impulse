import { Card, CardContent, CardHeader, TextField } from '@mui/material';
import type { NodeReplaceChange } from '@xyflow/react';
import type { OperationNode } from '..';

export interface TransformFormProps {
  node: OperationNode<'transform'>;
  onChange?: (change: NodeReplaceChange<OperationNode<'transform'>>) => void;
}

function TransformForm({ node, onChange }: TransformFormProps) {
  return (
    <Card>
      <CardHeader title="Edit Operation" />
      <CardContent>
        <TextField
          label="CEL"
          multiline
          fullWidth
          defaultValue={node.data.cel}
          onChange={(ev) => {
            node.data.cel = ev.target.value;
            onChange?.({
              type: 'replace',
              id: node.id,
              item: { ...node },
            });
          }}
        />
      </CardContent>
    </Card>
  );
}

export default TransformForm;
