import {
  Card,
  CardContent,
  CardHeader,
  IconButton,
  Stack,
  TextField,
} from '@mui/material';
import type { NodeChange, NodeRemoveChange } from '@xyflow/react';
import type { PropsWithChildren } from 'react';
import {
  MaterialSymbol,
  type SectionBufferNode,
  type SectionInputNode,
  type SectionInterfaceNode,
  type SectionOutputNode,
} from '../nodes';

export interface BaseEditSectionInterfaceFormProps {
  node: SectionInterfaceNode;
  onDelete?: (change: NodeRemoveChange) => void;
}

function BaseEditSectionInterfaceForm({
  node,
  onDelete,
  children,
}: PropsWithChildren<BaseEditSectionInterfaceFormProps>) {
  return (
    <Card>
      <CardHeader
        title="Edit"
        action={
          <IconButton
            color="error"
            onClick={() => onDelete?.({ type: 'remove', id: node.id })}
          >
            <MaterialSymbol symbol="delete" />
          </IconButton>
        }
      />
      <CardContent>{children}</CardContent>
    </Card>
  );
}

export interface EditSectionInputFormProps
  extends BaseEditSectionInterfaceFormProps {
  node: SectionInputNode;
  onChange?: (change: NodeChange<SectionInputNode>) => void;
}

export function EditSectionInputForm({
  node,
  onChange,
  onDelete,
}: EditSectionInputFormProps) {
  return (
    <BaseEditSectionInterfaceForm node={node} onDelete={onDelete}>
      <Stack spacing={2}>
        <TextField
          required
          label="Remapped Id"
          value={node.data.remappedId}
          onChange={(ev) => {
            const updatedNode = { ...node };
            updatedNode.data.remappedId = ev.target.value;
            onChange?.({
              type: 'replace',
              id: node.id,
              item: updatedNode,
            });
          }}
        />
      </Stack>
    </BaseEditSectionInterfaceForm>
  );
}

export interface EditSectionBufferFormProps
  extends BaseEditSectionInterfaceFormProps {
  node: SectionBufferNode;
  onChange?: (change: NodeChange<SectionBufferNode>) => void;
}

export function EditSectionBufferForm({
  node,
  onChange,
  onDelete,
}: EditSectionBufferFormProps) {
  return (
    <BaseEditSectionInterfaceForm node={node} onDelete={onDelete}>
      <Stack spacing={2}>
        <TextField
          required
          label="Remapped Id"
          value={node.data.remappedId}
          onChange={(ev) => {
            const updatedNode = { ...node };
            updatedNode.data.remappedId = ev.target.value;
            onChange?.({
              type: 'replace',
              id: node.id,
              item: updatedNode,
            });
          }}
        />
      </Stack>
    </BaseEditSectionInterfaceForm>
  );
}

export interface EditSectionOutputFormProps
  extends BaseEditSectionInterfaceFormProps {
  node: SectionOutputNode;
  onChange?: (change: NodeChange<SectionOutputNode>) => void;
}

export function EditSectionOutputForm({
  node,
  onChange,
  onDelete,
}: EditSectionOutputFormProps) {
  return (
    <BaseEditSectionInterfaceForm node={node} onDelete={onDelete}>
      <Stack spacing={2}>
        <TextField
          required
          label="Output Id"
          value={node.data.outputId}
          onChange={(ev) => {
            const updatedNode = { ...node };
            updatedNode.data.outputId = ev.target.value;
            onChange?.({
              type: 'replace',
              id: node.id,
              item: updatedNode,
            });
          }}
        />
      </Stack>
    </BaseEditSectionInterfaceForm>
  );
}
