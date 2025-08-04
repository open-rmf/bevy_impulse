import { FormControlLabel, Switch } from '@mui/material';
import BaseEditOperationForm, {
  type BaseEditOperationFormProps,
} from './base-edit-operation-form';

export type BufferFormProps = BaseEditOperationFormProps<'buffer'>;

function BufferForm(props: BufferFormProps) {
  return (
    <BaseEditOperationForm {...props}>
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
    </BaseEditOperationForm>
  );
}

export default BufferForm;
