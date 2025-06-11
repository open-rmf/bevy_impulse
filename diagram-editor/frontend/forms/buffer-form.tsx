import {
  Card,
  CardContent,
  CardHeader,
  FormControlLabel,
  Switch,
} from '@mui/material';
import type { NodeReplaceChange } from '@xyflow/react';
import type { OperationNode } from '../nodes';

export interface BufferFormProps {
  node: OperationNode<'buffer'>;
  onChange: (change: NodeReplaceChange<OperationNode<'buffer'>>) => void;
}

export function BufferForm({ node, onChange }: BufferFormProps) {
  return (
    <Card>
      <CardHeader title="Edit Operation" />
      <CardContent>
        <FormControlLabel
          control={
            <Switch
              checked={node.data.serialize ?? false}
              onChange={(_, checked) => {
                node.data.serialize = checked;
                onChange?.({
                  type: 'replace',
                  id: node.id,
                  item: node,
                });
              }}
            />
          }
          label="Serialize"
        />
      </CardContent>
    </Card>
  );
}

export default BufferForm;
