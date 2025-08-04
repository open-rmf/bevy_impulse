import { TextField } from '@mui/material';
import { parse } from 'cel-js';
import React from 'react';
import BaseEditOperationForm, {
  type BaseEditOperationFormProps,
} from './base-edit-operation-form';

export type TransformFormProps = BaseEditOperationFormProps<'transform'>;

function TransformForm(props: TransformFormProps) {
  const celError = React.useMemo(
    () => parse(props.node.data.op.cel).isSuccess,
    [props.node.data.op.cel],
  );
  return (
    <BaseEditOperationForm {...props}>
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

export default TransformForm;
