import { TextField } from '@mui/material';
import EditOperationForm, {
  type EditOperationFormProps,
} from './edit-operation-form';

function TransformForm(props: EditOperationFormProps<'transform'>) {
  return (
    <EditOperationForm {...props}>
      <TextField
        required
        label="id"
        defaultValue={props.node.id}
        onChange={(ev) => {
          props.onChange?.({
            type: 'replace',
            id: props.node.id,
            item: { ...props.node, id: ev.target.value },
          });
        }}
      />
      <TextField
        label="CEL"
        multiline
        fullWidth
        defaultValue={props.node.data.cel}
        onChange={(ev) => {
          props.node.data.cel = ev.target.value;
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

export default TransformForm;
