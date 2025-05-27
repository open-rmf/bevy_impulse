import AutoLayoutIcon from '@mui/icons-material/Dashboard';
import UploadIcon from '@mui/icons-material/UploadFile';
import {
  type Edge,
  Panel,
  ReactFlow,
  type ReactFlowInstance,
  StepEdge,
  addEdge,
  applyEdgeChanges,
  reconnectEdge,
  applyNodeChanges,
} from '@xyflow/react';
import React from 'react';

import {
  Button,
  ButtonGroup,
  Popover,
  type PopoverPosition,
  Tooltip,
  styled,
} from '@mui/material';
import AddOperation from './add-operation';
import {
  type DiagramEditorNode,
  NODE_TYPES,
  START_ID,
  TERMINATE_ID,
} from './nodes';
import { autoLayout } from './utils/auto-layout';
import { loadDiagramJson } from './utils/load-diagram';

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
  const reactFlowInstance =
    React.useRef<ReactFlowInstance<DiagramEditorNode> | null>(null);

  const [nodes, setNodes] = React.useState<DiagramEditorNode[]>(() => [
    {
      id: START_ID,
      type: 'start',
      position: { x: 0, y: 0 },
      selectable: false,
      data: {},
    },
    {
      id: TERMINATE_ID,
      type: 'terminate',
      position: { x: 0, y: 400 },
      selectable: false,
      data: {},
    },
  ]);
  const [edges, setEdges] = React.useState<Edge[]>([]);

  const [openAddOpPopover, setOpenAddOpPopover] = React.useState(false);
  const [addOpAnchorPos, setAddOpAnchorPos] = React.useState<PopoverPosition>({
    left: 0,
    top: 0,
  });

  const mouseDownTime = React.useRef(0);

  return (
    <>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeOrigin={[0.5, 0.5]}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        nodeTypes={NODE_TYPES}
        edgeTypes={{ default: StepEdge }}
        onInit={(instance) => {
          reactFlowInstance.current = instance;
        }}
        onNodesChange={(changes) =>
          setNodes((prev) => applyNodeChanges(changes, prev))
        }
        onEdgesChange={(changes) =>
          setEdges((prev) => applyEdgeChanges(changes, prev))
        }
        onConnect={(params) => setEdges((prev) => addEdge(params, prev))}
        onReconnect={(oldEdge, newConnection) =>
          setEdges((prev) => reconnectEdge(oldEdge, newConnection, prev))
        }
        onNodeClick={(ev) => {
          ev.stopPropagation();
        }}
        onEdgeClick={(ev) => ev.stopPropagation()}
        onPaneClick={(ev) => {
          // filter out erroneous click after connecting an edge
          const now = new Date().getTime();
          if (now - mouseDownTime.current > 200) {
            return;
          }
          setAddOpAnchorPos({ left: ev.clientX, top: ev.clientY });
          setOpenAddOpPopover(true);
        }}
        onMouseDownCapture={() => {
          mouseDownTime.current = new Date().getTime();
        }}
        onTouchStartCapture={() => {
          mouseDownTime.current = new Date().getTime();
        }}
        colorMode="dark"
        deleteKeyCode={'Delete'}
      >
        <Panel position="top-center">
          <ButtonGroup variant="contained">
            <Tooltip title="Auto Layout">
              <Button
                onClick={() => {
                  const startNode = nodes.find((n) => n.id === START_ID);
                  if (!startNode) {
                    console.error(
                      'error applying auto layout: cannot find start node',
                    );
                    return;
                  }
                  // reset all positions
                  for (const n of nodes) {
                    n.position = { ...startNode.position };
                  }

                  const startEdge = edges.find((e) => e.source === START_ID);
                  if (startEdge) {
                    const changes = autoLayout(
                      startEdge.target,
                      nodes,
                      startNode.position,
                    );
                    setNodes((prev) => applyNodeChanges(changes, prev));
                  }
                }}
              >
                <AutoLayoutIcon />
              </Button>
            </Tooltip>
            <Tooltip title="Load Diagram">
              {/* biome-ignore lint/a11y/useValidAriaRole: button used as a label, should have no role */}
              <Button component="label" role={undefined}>
                <UploadIcon />
                <VisuallyHiddenInput
                  type="file"
                  accept="application/json"
                  aria-label="load diagram"
                  onChange={async (ev) => {
                    if (ev.target.files) {
                      const graph = loadDiagramJson(
                        await ev.target.files[0].text(),
                      );
                      const changes = autoLayout(
                        graph.startNodeId,
                        graph.nodes,
                      );
                      setNodes(applyNodeChanges(changes, graph.nodes));
                      setEdges(graph.edges);
                      reactFlowInstance.current?.fitView();
                    }
                  }}
                  onClick={(ev) => {
                    // Reset the input value so that the same file can be loaded multiple times
                    (ev.target as HTMLInputElement).value = '';
                  }}
                />
              </Button>
            </Tooltip>
          </ButtonGroup>
        </Panel>
      </ReactFlow>
      <Popover
        open={openAddOpPopover}
        onClose={() => setOpenAddOpPopover(false)}
        anchorReference="anchorPosition"
        anchorPosition={addOpAnchorPos}
      >
        <AddOperation
          onAdd={(change) => {
            const newNode = change.item;
            let i = 1;
            let newId = newNode.id;
            while (nodes.findIndex((n) => n.id === newId) >= 0) {
              newId = `${newNode.id}_${i++}`;
            }
            newNode.id = newId;
            const newPos = reactFlowInstance.current?.screenToFlowPosition({
              x: addOpAnchorPos.left,
              y: addOpAnchorPos.top,
            });
            if (!newPos) {
              throw new Error(
                'failed to add operation: cannot determine position',
              );
            }
            newNode.position = newPos;
            setNodes((prev) => applyNodeChanges([change], prev));
            setOpenAddOpPopover(false);
          }}
        />
      </Popover>
    </>
  );
};

export default DiagramEditor;
