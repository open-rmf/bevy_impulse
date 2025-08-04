import {
  Card,
  CardContent,
  CardHeader,
  FormControl,
  IconButton,
  InputLabel,
  MenuItem,
  Select,
  Stack,
} from '@mui/material';
import type { EdgeChange, EdgeRemoveChange } from '@xyflow/react';
import React from 'react';
import type {
  BufferEdge,
  DiagramEditorEdge,
  EdgeOutputData,
  EdgeTypes,
  UnzipEdge,
} from '../edges';
import { MaterialSymbol } from '../nodes';
import { BufferEdgeInputForm } from './buffer-edge-form';
import SplitEdgeForm, { type SplitEdge } from './split-edge-form';
import UnzipEdgeForm from './unzip-edge-form';

const EDGE_TYPES_NAME = {
  buffer: 'Buffer',
  default: 'Data',
  forkResultErr: 'Error',
  forkResultOk: 'Ok',
  splitKey: 'Key',
  splitRemaining: 'Remaining',
  splitSeq: 'Sequence',
  streamOut: 'Stream Out',
  unzip: 'Unzip',
  // section: 'Section',
} satisfies Record<EdgeTypes, string>;

const EDGE_DEFAULT_DATA = {
  default: {},
  buffer: {},
  forkResultOk: {},
  forkResultErr: {},
  splitKey: { key: '' },
  splitSeq: { seq: 0 },
  splitRemaining: {},
  streamOut: { name: '' },
  unzip: { seq: 0 },
  // section: { output: '' },
} satisfies Record<EdgeTypes, EdgeOutputData>;

export function defaultEdgeOutputData(
  type: EdgeTypes,
): EdgeOutputData<EdgeTypes> {
  return { ...EDGE_DEFAULT_DATA[type] };
}

export interface EditEdgeFormProps {
  edge: DiagramEditorEdge;
  allowedEdgeTypes: EdgeTypes[];
  onChange?: (changes: EdgeChange<DiagramEditorEdge>) => void;
  onDelete?: (change: EdgeRemoveChange) => void;
}

function EditEdgeForm({
  edge,
  allowedEdgeTypes,
  onChange,
  onDelete,
}: EditEdgeFormProps) {
  const subForm = React.useMemo(() => {
    switch (edge.type) {
      case 'buffer': {
        return (
          <BufferEdgeInputForm edge={edge as BufferEdge} onChange={onChange} />
        );
      }
      case 'forkResultOk':
      case 'forkResultErr': {
        return null;
      }
      case 'splitKey':
      case 'splitRemaining':
      case 'splitSeq': {
        return <SplitEdgeForm edge={edge as SplitEdge} onChange={onChange} />;
      }
      case 'unzip': {
        return <UnzipEdgeForm edge={edge as UnzipEdge} onChange={onChange} />;
      }
      default: {
        return null;
      }
    }
  }, [edge, onChange]);

  const typeLabelId = React.useId();

  return (
    <Card sx={{ minWidth: 250 }}>
      <CardHeader
        title="Edit Connection"
        action={
          <IconButton
            color="error"
            onClick={() => onDelete?.({ type: 'remove', id: edge.id })}
          >
            <MaterialSymbol symbol="delete" />
          </IconButton>
        }
      />
      <CardContent>
        <Stack spacing={2}>
          <FormControl>
            <InputLabel id={typeLabelId}>Type</InputLabel>
            <Select
              labelId={typeLabelId}
              label="Type"
              value={edge.type}
              onChange={(ev) => {
                const newEdge: DiagramEditorEdge = {
                  ...edge,
                };
                newEdge.type = ev.target.value;
                newEdge.data = {
                  ...edge.data,
                  output: { ...defaultEdgeOutputData(newEdge.type) },
                };
                onChange?.({
                  type: 'replace',
                  id: edge.id,
                  item: newEdge,
                });
              }}
            >
              {allowedEdgeTypes.map((t) => (
                <MenuItem key={t} value={t}>
                  {EDGE_TYPES_NAME[t]}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
          {subForm}
        </Stack>
      </CardContent>
    </Card>
  );
}

export default EditEdgeForm;
