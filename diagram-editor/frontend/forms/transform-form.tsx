import { TextField } from '@mui/material';
import { parse } from 'cel-js';
import React from 'react';
import EditOperationForm, {
  type EditOperationFormProps,
} from './edit-operation-form';

function TransformForm(props: EditOperationFormProps<'transform'>) {
  const celError = React.useMemo(
    () => parse(props.node.data.op.cel).isSuccess,
    [props.node.data.op.cel],
  );
  return (
    <EditOperationForm {...props}>
      <TextField
        label="CEL"
        multiline
        fullWidth
        sx={{ width: 300 }}
        value={props.node.data.op.cel}
        slotProps={{
          htmlInput: {
            style: {
              fontFamily: 'monospace',
              whiteSpace: 'pre',
            },
          },
        }}
        error={!celError}
        onChange={(ev) => {
          const updatedNode = {
            ...props.node,
            data: {
              ...props.node.data,
              op: {
                ...props.node.data.op,
                cel: ev.target.value,
              },
            },
          };
          props.onChanges?.({
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
