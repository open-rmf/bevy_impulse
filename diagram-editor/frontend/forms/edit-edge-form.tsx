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
  EdgeData,
  EdgeTypes,
  SectionEdge,
  UnzipEdge,
} from '../edges';
import { MaterialSymbol } from '../nodes';
import { exhaustiveCheck } from '../utils/exhaustive-check';
import { BufferEdgeInputForm } from './buffer-edge-form';
import { DataInputForm } from './data-input-form';
import { SectionEdgeForm } from './section-form';
import SplitEdgeForm, { type SplitEdge } from './split-edge-form';
import UnzipEdgeForm from './unzip-edge-form';

const EDGE_TYPES_NAME = {
  buffer: 'Buffer',
  default: 'Default',
  forkResultErr: 'Error',
  forkResultOk: 'Ok',
  splitKey: 'Key',
  splitRemaining: 'Remaining',
  splitSeq: 'Sequence',
  streamOut: 'Stream Out',
  unzip: 'Unzip',
  section: 'Section',
} satisfies Record<EdgeTypes, string>;

const EDGE_DEFAULT_OUTPUT_DATA = {
  default: { output: {}, input: { type: 'default' } },
  buffer: { output: {}, input: { type: 'bufferSeq', seq: 0 } },
  forkResultOk: { output: {}, input: { type: 'default' } },
  forkResultErr: { output: {}, input: { type: 'default' } },
  splitKey: { output: { key: '' }, input: { type: 'default' } },
  splitSeq: { output: { seq: 0 }, input: { type: 'default' } },
  splitRemaining: { output: {}, input: { type: 'default' } },
  streamOut: { output: { name: '' }, input: { type: 'default' } },
  unzip: { output: { seq: 0 }, input: { type: 'default' } },
  section: { output: { output: '' }, input: { type: 'default' } },
} satisfies { [K in EdgeTypes]: EdgeData<K> };

export function defaultEdgeData(type: EdgeTypes): EdgeData {
  return { ...EDGE_DEFAULT_OUTPUT_DATA[type] };
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
      case 'default':
      case 'streamOut':
      case 'forkResultOk':
      case 'forkResultErr': {
        // these edges have no extra options
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
      case 'section': {
        return (
          <SectionEdgeForm edge={edge as SectionEdge} onChange={onChange} />
        );
      }
      default: {
        exhaustiveCheck(edge);
        throw new Error('unknown edge type');
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
                newEdge.data = defaultEdgeData(newEdge.type);
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
          <DataInputForm edge={edge} onChange={onChange} />
          {subForm}
        </Stack>
      </CardContent>
    </Card>
  );
}

export default EditEdgeForm;
