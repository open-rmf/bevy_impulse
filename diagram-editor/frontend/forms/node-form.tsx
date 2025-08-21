import { Autocomplete, TextField } from '@mui/material';
import { useMemo, useState } from 'react';
import { useRegistry } from '../registry-provider';
import BaseEditOperationForm, {
  type BaseEditOperationFormProps,
} from './base-edit-operation-form';

export type NodeFormProps = BaseEditOperationFormProps<'node'>;

function NodeForm(props: NodeFormProps) {
  const registry = useRegistry();
  const nodes = Object.keys(registry.nodes);
  const [configValue, setConfigValue] = useState(() =>
    props.node.data.op.config ? JSON.stringify(props.node.data.op.config) : '',
  );
  const configError = useMemo(() => {
    if (configValue === '') {
      return false;
    }
    try {
      JSON.parse(configValue);
      return false;
    } catch {
      return true;
    }
  }, [configValue]);

  return (
    <BaseEditOperationForm {...props}>
      <Autocomplete
        freeSolo
        autoSelect
        options={nodes}
        getOptionLabel={(option) => option}
        value={props.node.data.op.builder}
        onChange={(_, value) => {
          const updatedNode = { ...props.node };
          updatedNode.data.op.builder = value ?? '';
          props.onChange?.({
            type: 'replace',
            id: props.node.id,
            item: updatedNode,
          });
        }}
        renderInput={(params) => (
          <TextField {...params} required label="Builder" />
        )}
      />
      <TextField
        multiline
        rows={4}
        label="Config"
        value={configValue}
        onChange={(ev) => {
          setConfigValue(ev.target.value);
          try {
            const updatedNode = { ...props.node };
            updatedNode.data.op.config =
              ev.target.value === '' ? undefined : JSON.parse(ev.target.value);
            props.onChange?.({
              type: 'replace',
              id: props.node.id,
              item: updatedNode,
            });
          } catch {}
        }}
        error={configError}
        slotProps={{
          htmlInput: {
            sx: { fontFamily: 'monospace', whiteSpace: 'nowrap' },
          },
        }}
      />
    </BaseEditOperationForm>
  );
}

export default NodeForm;
