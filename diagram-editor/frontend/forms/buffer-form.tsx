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
            checked={props.node.data.serialize ?? false}
            onChange={(_, checked) => {
              props.node.data.serialize = checked;
              props.onChange?.({
                type: 'replace',
                id: props.node.id,
                item: { ...props.node },
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
