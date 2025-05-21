import UploadIcon from '@mui/icons-material/UploadFile';
import {
  type Edge,
  Panel,
  ReactFlow,
  StepEdge,
  addEdge,
  applyEdgeChanges,
  applyNodeChanges,
} from '@xyflow/react';
import React from 'react';

import { Button, ButtonGroup, styled } from '@mui/material';
import { type DiagramEditorNode, NODE_TYPES } from './nodes';
import { loadDiagramJson } from './utils/load-diagram-json';

const VisuallyHiddenInput = styled('input')({
  clip: 'rect(0 0 0 0)',
  clipPath: 'inset(50%)',
  height: 1,
  overflow: 'hidden',
  position: 'absolute',
  bottom: 0,
  left: 0,
  whiteSpace: 'nowrap',
  width: 1,
});

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
      onNodesChange={(changes) =>
        setNodes((prev) => applyNodeChanges(changes, prev))
      }
      onEdgesChange={(changes) =>
        setEdges((prev) => applyEdgeChanges(changes, prev))
      }
      onConnect={(params) => setEdges((prev) => addEdge(params, prev))}
      colorMode="dark"
    >
      <Panel position="top-center">
        <ButtonGroup>
          {/* biome-ignore lint/a11y/useValidAriaRole: button used as a label, should have no role */}
          <Button variant="contained" component="label" role={undefined}>
            <UploadIcon />
            <VisuallyHiddenInput
              type="file"
              aria-label="upload diagram"
              onChange={async (ev) => {
                if (ev.target.files) {
                  const graph = loadDiagramJson(
                    await ev.target.files[0].text(),
                  );
                  setNodes(graph.nodes);
                  setEdges(graph.edges);
                }
              }}
              onClick={(ev) => {
                (ev.target as HTMLInputElement).value = '';
              }}
            />
          </Button>
        </ButtonGroup>
      </Panel>
    </ReactFlow>
  );
};

export default DiagramEditor;
