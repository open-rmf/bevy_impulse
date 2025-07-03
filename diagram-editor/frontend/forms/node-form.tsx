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
        defaultValue={props.node.data.op.builder}
        onChange={(ev) => {
          const updatedNode = { ...props.node };
          updatedNode.data.op.builder = ev.target.value;
          props.onChanges?.([
            {
              type: 'replace',
              id: props.node.id,
              item: updatedNode,
            },
          ]);
        }}
      />
    </EditOperationForm>
  );
}

export default NodeForm;
