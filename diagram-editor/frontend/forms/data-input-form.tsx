import { Autocomplete, TextField } from '@mui/material';
import type { EdgeChange } from '@xyflow/react';
import { useMemo } from 'react';
import { type DiagramEditorEdge, isDataEdge } from '../edges';
import { useNodeManager } from '../node-manager';
import { isSectionNode } from '../nodes';
import { useRegistry } from '../registry-provider';
import { useTemplates } from '../templates-provider';
import type { SectionTemplate } from '../types/api';

function getTemplateInputs(template: SectionTemplate): string[] {
  if (!template.inputs) {
    return [];
  }
  if (Array.isArray(template.inputs)) {
    return template.inputs;
  } else {
    return Object.keys(template.inputs);
  }
}

export interface DataInputEdgeFormProps {
  edge: DiagramEditorEdge;
  onChange?: (changes: EdgeChange<DiagramEditorEdge>) => void;
}

export function DataInputForm({ edge, onChange }: DataInputEdgeFormProps) {
  const nodeManager = useNodeManager();
  const registry = useRegistry();
  const [templates, _setTemplates] = useTemplates();
  const targetNode = nodeManager.getNode(edge.target);

  const inputs = useMemo(() => {
    if (!targetNode || !isSectionNode(targetNode)) {
      return [];
    }

    if (typeof targetNode.data.op.builder === 'string') {
      const sectionBuilder = registry.sections[targetNode.data.op.builder];
      return Object.keys(sectionBuilder?.metadata.inputs || {});
    } else if (typeof targetNode.data.op.template === 'string') {
      const template = templates[targetNode.data.op.template];
      return template ? getTemplateInputs(template) : [];
    } else {
      return [];
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
          options={inputs}
          value={
            edge.data.input.type === 'sectionInput'
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
                  input: { type: 'sectionInput', inputId: value || '' },
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
