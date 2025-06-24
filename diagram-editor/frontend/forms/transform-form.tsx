import { TextField } from '@mui/material';
import EditOperationForm, {
  type EditOperationFormProps,
} from './edit-operation-form';

function TransformForm(props: EditOperationFormProps<'transform'>) {
  return (
    <EditOperationForm {...props}>
      <TextField
        label="CEL"
        multiline
        fullWidth
        defaultValue={props.node.data.op.cel}
        onChange={(ev) => {
          const updatedNode = { ...props.node };
          updatedNode.data.op.cel = ev.target.value;
          props.node.data.op.cel = ev.target.value;
          props.onChange?.({
            type: 'replace',
            id: props.node.id,
            item: updatedNode,
          });
        }}
      />
    </EditOperationForm>
  );
}

export default TransformForm;
