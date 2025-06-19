import DeleteIcon from '@mui/icons-material/Delete';
import {
  Button,
  Card,
  CardContent,
  CardHeader,
  DialogActions,
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
      <CardHeader title="Edit Operation" />
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
      <DialogActions>
        <Button
          variant="contained"
          startIcon={<DeleteIcon />}
          onClick={() => onDelete?.({ type: 'remove', id: node.id })}
        >
          Delete
        </Button>
      </DialogActions>
    </Card>
  );
}

export default EditOperationForm;
