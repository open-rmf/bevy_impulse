import { Autocomplete, TextField } from '@mui/material';
import type { EdgeChange } from '@xyflow/react';
import type { StreamOutEdge } from '../edges';
import type { NodeManager } from '../node-manager';
import { isOperationNode } from '../nodes';
import { useRegistry } from '../registry-provider';

export interface StreamOutEdgeFormProps {
  edge: StreamOutEdge;
  nodeManager: NodeManager;
  onChange?: (change: EdgeChange<StreamOutEdge>) => void;
}

export function StreamOutEdgeForm({
  edge,
  nodeManager,
  onChange,
}: StreamOutEdgeFormProps) {
  const registry = useRegistry();
  const sourceNode = nodeManager.getNode(edge.source);
  const builderMetadata =
    isOperationNode(sourceNode) && sourceNode.data.op.type === 'node'
      ? registry.nodes[sourceNode.data.op.builder]
      : null;
  const streams = builderMetadata ? Object.keys(builderMetadata.streams) : [];

  return (
    <>
      <Autocomplete
        freeSolo
        autoSelect
        fullWidth
        options={streams}
        getOptionLabel={(option) => option}
        value={edge.data.output.streamId}
        onChange={(_, value) => {
          const newStreamId = value || '';
          onChange?.({
            type: 'replace',
            id: edge.id,
            item: {
              ...edge,
              data: { ...edge.data, output: { streamId: newStreamId } },
            },
          });
        }}
        renderInput={(params) => (
          <TextField {...params} required label="Stream" />
        )}
      />
    </>
  );
}
