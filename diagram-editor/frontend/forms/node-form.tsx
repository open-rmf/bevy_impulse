import { Autocomplete, TextField } from '@mui/material';
import { useRegistry } from '../registry-provider';
import BaseEditOperationForm, {
  type BaseEditOperationFormProps,
} from './base-edit-operation-form';

export type NodeFormProps = BaseEditOperationFormProps<'node'>;

function NodeForm(props: NodeFormProps) {
  const registry = useRegistry();
  const nodes = Object.keys(registry.nodes);
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
          <TextField {...params} required label="builder" />
        )}
      />
    </BaseEditOperationForm>
  );
}

export default NodeForm;
