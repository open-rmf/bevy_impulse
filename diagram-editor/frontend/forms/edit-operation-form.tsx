import DeleteIcon from '@mui/icons-material/Delete';
import {
  Card,
  CardContent,
  CardHeader,
  IconButton,
  Stack,
  TextField,
} from '@mui/material';
import type { NodeRemoveChange, NodeReplaceChange } from '@xyflow/react';
import type React from 'react';
import type { OperationNode, OperationNodeTypes } from '../types';

export interface EditOperationFormProps<
  NodeType extends OperationNodeTypes = OperationNodeTypes,
> {
  node: OperationNode<NodeType>;
  onChange?: (change: NodeReplaceChange<OperationNode>) => void;
  onDelete?: (change: NodeRemoveChange) => void;
}

function EditOperationForm({
  node,
  onChange,
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
            <DeleteIcon />
          </IconButton>
        }
      />
      <CardContent>
        <Stack spacing={2}>
          <TextField
            required
            label="id"
            defaultValue={node.id}
            onChange={(ev) => {
              onChange?.({
                type: 'replace',
                id: node.id,
                item: { ...node, id: ev.target.value },
              });
            }}
          />
          {children}
        </Stack>
      </CardContent>
    </Card>
  );
}

export default EditOperationForm;
