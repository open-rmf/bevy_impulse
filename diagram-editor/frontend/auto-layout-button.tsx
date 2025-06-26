import AutoLayoutIcon from '@mui/icons-material/Dashboard';
import { Button, Tooltip } from '@mui/material';
import { type NodeChange, type ReactFlowState, useStore } from '@xyflow/react';
import React from 'react';
import { START_ID } from './nodes';
import type { DiagramEditorEdge, DiagramEditorNode } from './types';
import { autoLayout } from './utils';

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
          const startNode = nodes.find((n) => n.id === START_ID);
          if (!startNode) {
            console.error('error applying auto layout: cannot find start node');
            return;
          }
          // reset all positions
          for (const n of nodes) {
            n.position = { ...startNode.position };
          }

          const changes = autoLayout(START_ID, nodes, edges, {
            rootPosition: startNode.position,
          });
          onNodeChanges(changes);
        }}
      >
        <AutoLayoutIcon />
      </Button>
    </Tooltip>
  );
}

export default React.memo(AutoLayoutButton);
