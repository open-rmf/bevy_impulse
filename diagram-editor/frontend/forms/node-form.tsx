import { Autocomplete, TextField } from '@mui/material';
import { useRegistry } from '../registry-provider';
import EditOperationForm, {
  type EditOperationFormProps,
} from './edit-operation-form';

function NodeForm(props: EditOperationFormProps<'node'>) {
  const registry = useRegistry();
  const nodes = Object.keys(registry.nodes);
  return (
    <EditOperationForm {...props}>
      <Autocomplete
        freeSolo
        autoSelect
        options={nodes}
        getOptionLabel={(option) => option}
        value={props.node.data.op.builder}
        onChange={(_, value) => {
          const updatedNode = { ...props.node };
          updatedNode.data.op.builder = value ?? '';
          props.onChanges?.([
            {
              type: 'replace',
              id: props.node.id,
              item: updatedNode,
            },
          ]);
        }}
        renderInput={(params) => (
          <TextField {...params} required label="builder" />
        )}
      />
    </EditOperationForm>
  );
}

export default NodeForm;
