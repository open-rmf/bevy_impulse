import {
  type Edge,
  type OnConnect,
  type OnEdgesChange,
  type OnNodesChange,
  ReactFlow,
  StepEdge,
  addEdge,
  applyEdgeChanges,
  applyNodeChanges,
} from '@xyflow/react';
import React from 'react';

import { type DiagramEditorNode, NODE_TYPES } from './nodes';

const DiagramEditor = () => {
  const [nodes, setNodes] = React.useState<DiagramEditorNode[]>(() => [
    {
      id: 'builtin:start',
      type: 'start',
      position: { x: 0, y: 0 },
      selectable: false,
      data: {},
    },
    {
      id: 'builtin:terminate',
      type: 'terminate',
      position: { x: 0, y: 400 },
      selectable: false,
      data: {},
    },
  ]);
  const [edges, setEdges] = React.useState<Edge[]>([]);

  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      nodeOrigin={[0.5, 0.5]}
      fitView
      nodeTypes={NODE_TYPES}
      edgeTypes={{ default: StepEdge }}
      onNodesChange={React.useCallback<OnNodesChange<DiagramEditorNode>>(
        (changes) => setNodes((prev) => applyNodeChanges(changes, prev)),
        [],
      )}
      onEdgesChange={React.useCallback<OnEdgesChange>(
        (changes) => setEdges((prev) => applyEdgeChanges(changes, prev)),
        [],
      )}
      onConnect={React.useCallback<OnConnect>(
        (params) => setEdges((prev) => addEdge(params, prev)),
        [],
      )}
      colorMode="dark"
    />
  );
};

export default DiagramEditor;
