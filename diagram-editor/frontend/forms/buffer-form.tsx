import {
  FormControl,
  FormControlLabel,
  InputLabel,
  MenuItem,
  Select,
  type SelectChangeEvent,
  Stack,
  Switch,
  TextField,
} from '@mui/material';
import React from 'react';
import type { RetentionPolicy } from '../types/api';
import { getRetentionMode, type RetentionMode } from '../utils/buffer-settings';
import BaseEditOperationForm, {
  type BaseEditOperationFormProps,
} from './base-edit-operation-form';

export type BufferFormProps = BaseEditOperationFormProps<'buffer'>;

function BufferForm(props: BufferFormProps) {
  const { node, onChange } = props;
  const { op } = node.data;
  const retentionLabelId = React.useId();

  const retention = op.settings?.retention ?? { keep_last: 1 };
  const retentionMode = getRetentionMode(retention);
  const retentionValue =
    retention === 'keep_all' ? 1 : Object.values(retention)[0];

  const handleRetentionModeChange = (e: SelectChangeEvent<RetentionMode>) => {
    const mode = e.target.value;
    let newRetention: RetentionPolicy;
    if (mode === 'keep_all') {
      newRetention = 'keep_all';
    } else if (mode === 'keep_first') {
      newRetention = { keep_first: retentionValue };
    } else if (mode === 'keep_last') {
      newRetention = { keep_last: retentionValue };
    } else {
      throw new Error(`Invalid retention mode: ${mode}`);
    }

    const updatedNode = { ...node };
    updatedNode.data.op = {
      ...op,
      settings: {
        ...op.settings,
        retention: newRetention,
      },
    };
    onChange?.({
      type: 'replace',
      id: node.id,
      item: updatedNode,
    });
  };

  const handleRetentionValueChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>,
  ) => {
    const value = parseInt(e.target.value, 10);
    if (value < 1 || !Number.isInteger(value)) return;

    let newRetention: RetentionPolicy;
    if (retentionMode === 'keep_first') {
      newRetention = { keep_first: value };
    } else if (retentionMode === 'keep_last') {
      newRetention = { keep_last: value };
    } else {
      throw new Error(
        `Invalid retention mode for value change: ${retentionMode}`,
      );
    }

    const updatedNode = { ...node };
    updatedNode.data.op = {
      ...op,
      settings: {
        ...op.settings,
        retention: newRetention,
      },
    };
    onChange?.({
      type: 'replace',
      id: node.id,
      item: updatedNode,
    });
  };

  return (
    <BaseEditOperationForm {...props}>
      <Stack direction="row" spacing={2} minWidth={300}>
        <FormControl fullWidth>
          <InputLabel id={retentionLabelId}>Retention Policy</InputLabel>
          <Select
            labelId={retentionLabelId}
            value={retentionMode}
            label="Retention Policy"
            onChange={handleRetentionModeChange}
          >
            <MenuItem value="keep_last">Keep Last</MenuItem>
            <MenuItem value="keep_first">Keep First</MenuItem>
            <MenuItem value="keep_all">Keep All</MenuItem>
          </Select>
        </FormControl>
        {(retentionMode === 'keep_first' || retentionMode === 'keep_last') && (
          <TextField
            type="number"
            label="Amount"
            value={retentionValue}
            onChange={handleRetentionValueChange}
            slotProps={{
              htmlInput: {
                min: 1,
                step: 1,
              },
            }}
          />
        )}
      </Stack>
      <FormControlLabel
        control={
          <Switch
            checked={op.serialize ?? false}
            onChange={(_, checked) => {
              const updatedNode = { ...node };
              updatedNode.data.op = {
                ...op,
                serialize: checked,
              };
              onChange?.({
                type: 'replace',
                id: node.id,
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
