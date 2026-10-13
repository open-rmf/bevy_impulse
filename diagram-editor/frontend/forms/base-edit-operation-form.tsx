import {
  Card,
  CardContent,
  CardHeader,
  IconButton,
  Stack,
  TextField,
} from '@mui/material';
import type { NodeChange, NodeRemoveChange } from '@xyflow/react';
import { type PropsWithChildren, useState } from 'react';
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
}: PropsWithChildren<BaseEditOperationFormProps>) {
  const [idCleared, setIdCleared] = useState(false);
  const automatic = node.data.op.extensions?.['crossflow-auto-id'] === true;
  const updateId = (opId: string) => {
    onChange?.({
      type: 'replace',
      id: node.id,
      item: {
        ...node,
        data: {
          ...node.data,
          opId,
          op: {
            ...node.data.op,
            extensions: {
              ...node.data.op.extensions,
              'crossflow-auto-id': opId === '',
            },
          },
        },
      },
    });
  };
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
            value={idCleared ? '' : node.data.opId}
            sx={{
              '& input': {
                color: automatic ? 'text.secondary' : 'text.primary',
              },
            }}
            onChange={(ev) => {
              const value = ev.target.value;
              setIdCleared(value === '');
              if (value !== '') updateId(value);
            }}
            onBlur={() => {
              if (idCleared && !automatic) updateId('');
              setIdCleared(false);
            }}
          />
          {children}
        </Stack>
      </CardContent>
    </Card>
  );
}

export default BaseEditOperationForm;
