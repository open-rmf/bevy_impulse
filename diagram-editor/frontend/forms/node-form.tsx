import { TextField } from '@mui/material';
import EditOperationForm, {
  type EditOperationFormProps,
} from './edit-operation-form';

function NodeForm(props: EditOperationFormProps<'node'>) {
  return (
    <EditOperationForm {...props}>
      <TextField
        required
        label="builder"
        defaultValue={props.node.data.builder}
        onChange={(ev) => {
          props.node.data.builder = ev.target.value;
          props.onChange?.({
            type: 'replace',
            id: props.node.id,
            item: { ...props.node },
          });
        }}
      />
    </EditOperationForm>
  );
}

export default NodeForm;
