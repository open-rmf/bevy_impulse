import { Checkbox, FormControlLabel } from '@mui/material';
import BaseEditOperationForm, {
  type BaseEditOperationFormProps,
} from './base-edit-operation-form';

export type JoinFormProps = BaseEditOperationFormProps<'join'>;

function JoinForm(props: JoinFormProps) {
  const op = props.node.data.op;

  return (
    <BaseEditOperationForm {...props}>
      <FormControlLabel
        label="Serialize"
        control={<Checkbox checked={!!op.serialize} />}
        onChange={(_, checked) => {
          const updatedNode = { ...props.node };
          updatedNode.data.op = { ...updatedNode.data.op, serialize: checked };
          props.onChange?.({
            type: 'replace',
            id: props.node.id,
            item: updatedNode,
          });
        }}
      />
    </BaseEditOperationForm>
  );
}

export default JoinForm;
