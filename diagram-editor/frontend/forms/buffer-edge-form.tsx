import {
  Autocomplete,
  FormControl,
  InputLabel,
  MenuItem,
  Select,
  TextField,
} from '@mui/material';
import type { EdgeChange } from '@xyflow/react';
import { useId, useMemo } from 'react';
import { type BufferEdge, BufferPullType } from '../edges';
import { useNodeManager } from '../node-manager';
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

function defaultInputData(
  inputType: BufferEdge['data']['input']['type'],
): BufferEdge['data']['input'] {
  switch (inputType) {
    case 'bufferKey': {
      return { type: 'bufferKey', key: '' };
    }
    case 'bufferSeq': {
      return { type: 'bufferSeq', seq: 0 };
    }
    case 'sectionBuffer': {
      return { type: 'sectionBuffer', inputId: '' };
    }
    default:
      exhaustiveCheck(inputType);
      throw new Error('unknown buffer edge input type');
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
  const labelId = useId();
  const nodeManager = useNodeManager();
  const targetNode = nodeManager.tryGetNode(edge.target);
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

  return (
    <>
      <FormControl>
        <InputLabel id={labelId}>Slot</InputLabel>
        <Select
          labelId={labelId}
          label="Slot"
          value={edge.data.input.type}
          onChange={(ev) => {
            onChange?.({
              type: 'replace',
              id: edge.id,
              item: {
                ...edge,
                data: {
                  ...edge.data,
                  input: defaultInputData(ev.target.value),
                },
              } as BufferEdge,
            });
          }}
        >
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
      {nodeManager.getNode(edge.target).type === 'join' && (
        <FormControl>
          <InputLabel id={`${labelId}-pull-type`}>Pull Type</InputLabel>
          <Select
            labelId={labelId}
            label="Pull Type"
            value={edge.data.input.pull_type || BufferPullType.Pull}
            onChange={(ev) => {
              onChange?.({
                type: 'replace',
                id: edge.id,
                item: {
                  ...edge,
                  data: {
                    ...edge.data,
                    input: {
                      ...edge.data.input,
                      pull_type: ev.target.value,
                    },
                  },
                } as BufferEdge,
              });
            }}
          >
            <MenuItem value={BufferPullType.Pull}>Pull</MenuItem>
            <MenuItem value={BufferPullType.Clone}>Clone</MenuItem>
          </Select>
        </FormControl>
      )}
    </>
  );
}
