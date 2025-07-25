import { Button, Tooltip } from '@mui/material';
import { type NodeChange, type ReactFlowState, useStore } from '@xyflow/react';
import React from 'react';
import type { DiagramEditorEdge } from './edges';
import type { DiagramEditorNode } from './nodes';
import { MaterialSymbol } from './nodes/icons';
import { autoLayout } from './utils';
import { LAYOUT_OPTIONS } from './utils/layout';

export interface AutoLayoutButtonProps {
  onNodeChanges: (changes: NodeChange<DiagramEditorNode>[]) => void;
}

const nodesEdgesSelector = (state: ReactFlowState) => ({
  nodes: state.nodes as DiagramEditorNode[],
  edges: state.edges as DiagramEditorEdge[],
});

function AutoLayoutButton({ onNodeChanges }: AutoLayoutButtonProps) {
  const { nodes, edges } = useStore(nodesEdgesSelector);
  return (
    <Tooltip title="Auto Layout">
      <Button
        onClick={() => {
          const changes = autoLayout(nodes, edges, LAYOUT_OPTIONS);
          onNodeChanges(changes);
        }}
      >
        <MaterialSymbol symbol="dashboard" />
      </Button>
    </Tooltip>
  );
}

export default React.memo(AutoLayoutButton);
