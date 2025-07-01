import {
  Card,
  CardContent,
  CardHeader,
  IconButton,
  Stack,
  TextField,
} from '@mui/material';
import type { NodeChange, NodeRemoveChange } from '@xyflow/react';
import type React from 'react';
import { MaterialSymbol } from '../nodes/icons';
import type { OperationNode, OperationNodeTypes } from '../types';

export interface EditOperationFormProps<
  NodeType extends OperationNodeTypes = OperationNodeTypes,
> {
  node: OperationNode<NodeType>;
  onChanges?: (changes: NodeChange<OperationNode>[]) => void;
  onDelete?: (change: NodeRemoveChange) => void;
}

function EditOperationForm({
  node,
  onChanges,
  onDelete,
  children,
}: React.PropsWithChildren<EditOperationFormProps>) {
  return (
    <Card>
      <CardHeader
        title="Edit Operation"
        action={
          <IconButton
            color="error"
            onClick={() => onDelete?.({ type: 'remove', id: node.id })}
          >
            <MaterialSymbol symbol="delete" />
          </IconButton>
        }
      />
      <CardContent>
        <Stack spacing={2}>
          <TextField
            required
            label="id"
            value={node.data.opId}
            onChange={(ev) => {
              const updatedNode = { ...node };
              updatedNode.data.opId = ev.target.value;
              onChanges?.([
                {
                  type: 'replace',
                  id: node.id,
                  item: updatedNode,
                },
              ]);
            }}
          />
          {children}
        </Stack>
      </CardContent>
    </Card>
  );
}

export default EditOperationForm;
