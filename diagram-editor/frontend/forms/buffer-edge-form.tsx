import {
  Autocomplete,
  FormControl,
  InputLabel,
  MenuItem,
  Select,
  TextField,
} from '@mui/material';
import { type EdgeChange, useReactFlow } from '@xyflow/react';
import { useId, useMemo } from 'react';
import type { BufferEdge, DiagramEditorEdge } from '../edges';
import type { DiagramEditorNode } from '../nodes';
import { useRegistry } from '../registry-provider';
import { useTemplates } from '../templates-provider';
import type { SectionTemplate } from '../types/api';
import { exhaustiveCheck } from '../utils/exhaustive-check';

function getTemplateBuffers(template: SectionTemplate): string[] {
  if (!template.buffers) {
    return [];
  }
  if (Array.isArray(template.buffers)) {
    return template.buffers;
  } else {
    return Object.keys(template.buffers);
  }
}

export interface BufferEdgeInputFormProps {
  edge: BufferEdge;
  onChange?: (changes: EdgeChange<BufferEdge>) => void;
}

export function BufferEdgeInputForm({
  edge,
  onChange,
}: BufferEdgeInputFormProps) {
  const handleDataChange = (
    event: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>,
  ) => {
    switch (edge.data.input.type) {
      case 'bufferKey': {
        const newKey = event.target.value;
        onChange?.({
          type: 'replace',
          id: edge.id,
          item: {
            ...edge,
            data: { ...edge.data, input: { type: 'bufferKey', key: newKey } },
          },
        });
        break;
      }
      case 'bufferSeq': {
        const newSeq = Number.parseInt(event.target.value, 10);
        if (!Number.isNaN(newSeq)) {
          onChange?.({
            type: 'replace',
            id: edge.id,
            item: {
              ...edge,
              data: { ...edge.data, input: { type: 'bufferSeq', seq: newSeq } },
            },
          });
        }
        break;
      }
      case 'sectionBuffer': {
        const newBufferId = event.target.value;
        onChange?.({
          type: 'replace',
          id: edge.id,
          item: {
            ...edge,
            data: {
              ...edge.data,
              input: { type: 'sectionBuffer', inputId: newBufferId },
            },
          },
        });
        break;
      }
      default: {
        exhaustiveCheck(edge.data.input);
        throw new Error('unknown edge input');
      }
    }
  };

  const labelId = useId();

  const reactFlow = useReactFlow<DiagramEditorNode, DiagramEditorEdge>();
  const targetNode = reactFlow.getNode(edge.target);
  const targetIsSection = targetNode?.type === 'section';
  const registry = useRegistry();
  const [templates, _setTemplates] = useTemplates();

  const sectionBuffers = useMemo(() => {
    if (!targetNode || targetNode.type !== 'section') {
      return [];
    }
    if (typeof targetNode.data.op.builder === 'string') {
      const sectionRegistration = registry.sections[targetNode.data.op.builder];
      return sectionRegistration
        ? Object.keys(sectionRegistration.metadata.buffers)
        : [];
    } else if (typeof targetNode.data.op.template === 'string') {
      const template = templates[targetNode.data.op.template];
      return template ? getTemplateBuffers(template) : [];
    } else {
      return [];
    }
  }, [targetNode, registry, templates]);

  return (
    <>
      <FormControl>
        <InputLabel id={labelId}>Slot</InputLabel>
        <Select labelId={labelId} label="Slot" value={edge.data.input.type}>
          {!targetIsSection && <MenuItem value="bufferSeq">Index</MenuItem>}
          {!targetIsSection && <MenuItem value="bufferKey">Key</MenuItem>}
          {targetIsSection && (
            <MenuItem value="sectionBuffer">Section Buffer</MenuItem>
          )}
        </Select>
      </FormControl>
      {edge.data.input === undefined ||
        (edge.data.input.type === 'bufferSeq' && (
          <TextField
            label="Index"
            type="number"
            value={edge.data.input.seq}
            onChange={handleDataChange}
            fullWidth
          />
        ))}
      {edge.data.input.type === 'bufferKey' && (
        <TextField
          label="Key"
          value={edge.data.input.key}
          onChange={handleDataChange}
          fullWidth
        />
      )}
      {edge.data.input.type === 'sectionBuffer' && (
        <Autocomplete
          freeSolo
          autoSelect
          options={sectionBuffers}
          value={edge.data.input.inputId}
          onChange={(_, value) => {
            onChange?.({
              type: 'replace',
              id: edge.id,
              item: {
                ...edge,
                data: {
                  output: edge.data.output,
                  input: { type: 'sectionBuffer', inputId: value || '' },
                },
              } as BufferEdge,
            });
          }}
          renderInput={(params) => (
            <TextField {...params} required label="Section Buffer" />
          )}
        />
      )}
    </>
  );
}
