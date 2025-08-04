import {
  Autocomplete,
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
import { useRegistry } from '../registry-provider';
import { useTemplates } from '../templates-provider';
import BaseEditOperationForm, {
  type BaseEditOperationFormProps,
} from './base-edit-operation-form';

export interface BaseSectionInterfaceFormProps {
  node: SectionInterfaceNode;
  onDelete?: (change: NodeRemoveChange) => void;
}

function BaseSectionInterfaceForm({
  node,
  onDelete,
  children,
}: PropsWithChildren<BaseSectionInterfaceFormProps>) {
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

export interface SectionInputFormProps extends BaseSectionInterfaceFormProps {
  node: SectionInputNode;
  onChange?: (change: NodeChange<SectionInputNode>) => void;
}

export function SectionInputForm({
  node,
  onChange,
  onDelete,
}: SectionInputFormProps) {
  return (
    <BaseSectionInterfaceForm node={node} onDelete={onDelete}>
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
    </BaseSectionInterfaceForm>
  );
}

export interface SectionBufferFormProps extends BaseSectionInterfaceFormProps {
  node: SectionBufferNode;
  onChange?: (change: NodeChange<SectionBufferNode>) => void;
}

export function SectionBufferForm({
  node,
  onChange,
  onDelete,
}: SectionBufferFormProps) {
  return (
    <BaseSectionInterfaceForm node={node} onDelete={onDelete}>
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
    </BaseSectionInterfaceForm>
  );
}

export interface SectionOutputFormProps extends BaseSectionInterfaceFormProps {
  node: SectionOutputNode;
  onChange?: (change: NodeChange<SectionOutputNode>) => void;
}

export function SectionOutputForm({
  node,
  onChange,
  onDelete,
}: SectionOutputFormProps) {
  return (
    <BaseSectionInterfaceForm node={node} onDelete={onDelete}>
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
    </BaseSectionInterfaceForm>
  );
}

export type SectionFormProps = BaseEditOperationFormProps<'section'>;

export function SectionForm(props: SectionFormProps) {
  const registry = useRegistry();
  const [templates, _setTemplates] = useTemplates();
  const sectionBuilders = Object.keys(registry.sections);
  const sectionTemplates = Object.keys(templates);
  const sections = [...sectionBuilders, ...sectionTemplates].sort();

  return (
    <BaseEditOperationForm {...props}>
      <Autocomplete
        freeSolo
        autoSelect
        options={sections}
        getOptionLabel={(option) => option}
        value={
          (props.node.data.op.builder ||
            props.node.data.op.template ||
            '') as string
        }
        onChange={(_, value) => {
          if (value === null) {
            return;
          }

          const updatedNode = { ...props.node };
          if (sectionBuilders.includes(value)) {
            updatedNode.data.op.builder = value;
            delete updatedNode.data.op.template;
          } else if (sectionTemplates.includes(value)) {
            updatedNode.data.op.template = value;
            delete updatedNode.data.op.builder;
          } else {
            // unable to determine if selected option is a builder or template, assume template.
            updatedNode.data.op.template = value;
            delete updatedNode.data.op.builder;
          }
          props.onChange?.({
            type: 'replace',
            id: props.node.id,
            item: updatedNode,
          });
        }}
        renderInput={(params) => (
          <TextField {...params} required label="builder/template" />
        )}
      />
    </BaseEditOperationForm>
  );
}
