import { Autocomplete, TextField } from '@mui/material';
import type { EdgeChange } from '@xyflow/react';
import { useMemo } from 'react';
import { type DiagramEditorEdge, isDataEdge } from '../edges';
import { useNodeManager } from '../node-manager';
import { isSectionNode } from '../nodes';
import { useRegistry } from '../registry-provider';
import { useTemplates } from '../templates-provider';
import type { SectionTemplate } from '../types/api';

function slotNames(slots: SectionTemplate['inputs']): string[] {
  return Array.isArray(slots) ? slots : Object.keys(slots || {});
}

export interface DataInputEdgeFormProps {
  edge: DiagramEditorEdge;
  onChange?: (changes: EdgeChange<DiagramEditorEdge>) => void;
}

export function DataInputForm({ edge, onChange }: DataInputEdgeFormProps) {
  const nodeManager = useNodeManager();
  const registry = useRegistry();
  const [templates, _setTemplates] = useTemplates();
  const targetNode = nodeManager.tryGetNode(edge.target);
  const bufferAlias =
    nodeManager.tryGetNode(edge.source)?.type === 'sectionBuffer';

  const { inputs, buffers } = useMemo(() => {
    if (!targetNode || !isSectionNode(targetNode)) {
      return { inputs: [], buffers: [] };
    }

    if (typeof targetNode.data.op.builder === 'string') {
      const sectionBuilder = registry.sections[targetNode.data.op.builder];
      return {
        inputs: Object.keys(sectionBuilder?.interface.inputs || {}),
        buffers: Object.keys(sectionBuilder?.interface.buffers || {}),
      };
    } else if (typeof targetNode.data.op.template === 'string') {
      const template = templates[targetNode.data.op.template];
      return {
        inputs: slotNames(template?.inputs),
        buffers: slotNames(template?.buffers),
      };
    } else {
      return { inputs: [], buffers: [] };
    }
  }, [targetNode, registry, templates]);

  if (!isDataEdge(edge)) {
    return null;
  }

  return (
    <>
      {targetNode?.type === 'section' && (
        <Autocomplete
          freeSolo
          autoSelect
          options={bufferAlias ? buffers : [...inputs, ...buffers]}
          value={
            edge.data.input.type === 'sectionInput' ||
            edge.data.input.type === 'sectionBuffer'
              ? edge.data.input.inputId
              : ''
          }
          onChange={(_, value) => {
            onChange?.({
              type: 'replace',
              id: edge.id,
              item: {
                ...edge,
                data: {
                  output: edge.data.output,
                  input: {
                    type:
                      bufferAlias || buffers.includes(value || '')
                        ? 'sectionBuffer'
                        : 'sectionInput',
                    inputId: value || '',
                  },
                },
              } as DiagramEditorEdge,
            });
          }}
          renderInput={(params) => (
            <TextField {...params} required label="Section Input" />
          )}
        />
      )}
    </>
  );
}
