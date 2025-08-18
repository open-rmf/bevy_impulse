import { TextField } from '@mui/material';
import BaseEditOperationForm, {
  type BaseEditOperationFormProps,
} from './base-edit-operation-form';

export type StreamOutFormProps = BaseEditOperationFormProps<'stream_out'>;

export function StreamOutForm(props: StreamOutFormProps) {
  return (
    <BaseEditOperationForm {...props}>
      <TextField
        required
        label="Stream"
        fullWidth
        value={props.node.data.op.name}
        onChange={(ev) => {
          const updatedNode = { ...props.node };
          updatedNode.data.op.name = ev.target.value;
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
