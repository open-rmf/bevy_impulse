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
import type { OperationNode, OperationNodeTypes } from '../nodes';
import { MaterialSymbol } from '../nodes';

export interface BaseEditOperationFormProps<
  NodeType extends OperationNodeTypes = OperationNodeTypes,
> {
  node: OperationNode<NodeType>;
  onChange?: (changes: NodeChange<OperationNode>) => void;
  onDelete?: (change: NodeRemoveChange) => void;
}

function BaseEditOperationForm({
  node,
  onChange,
  onDelete,
  children,
}: React.PropsWithChildren<BaseEditOperationFormProps>) {
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
            label="id"
            fullWidth
            value={
              node.data.op.extensions?.['crossflow-auto-id'] === true
                ? ''
                : node.data.opId
            }
            placeholder={node.data.opId}
            helperText="Leave blank to generate an ID automatically."
            onChange={(ev) => {
              const updatedNode = {
                ...node,
                data: {
                  ...node.data,
                  opId: ev.target.value,
                  op: {
                    ...node.data.op,
                    extensions: {
                      ...node.data.op.extensions,
                      'crossflow-auto-id': ev.target.value === '',
                    },
                  },
                },
              };
              onChange?.({
                type: 'replace',
                id: node.id,
                item: updatedNode,
              });
            }}
          />
          {children}
        </Stack>
      </CardContent>
    </Card>
  );
}

export default BaseEditOperationForm;
