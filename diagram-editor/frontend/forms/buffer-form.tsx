import { FormControlLabel, Switch } from '@mui/material';
import EditOperationForm, {
  type EditOperationFormProps,
} from './edit-operation-form';

export function BufferForm(props: EditOperationFormProps<'buffer'>) {
  return (
    <EditOperationForm {...props}>
      <FormControlLabel
        control={
          <Switch
            checked={props.node.data.op.serialize ?? false}
            onChange={(_, checked) => {
              const updatedNode = { ...props.node };
              updatedNode.data.op.serialize = checked;
              props.onChange?.({
                type: 'replace',
                id: props.node.id,
                item: updatedNode,
              });
            }}
          />
        }
        label="Serialize"
      />
    </EditOperationForm>
  );
}

export default BufferForm;
