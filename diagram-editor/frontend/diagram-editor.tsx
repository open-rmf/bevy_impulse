import AutoLayoutIcon from '@mui/icons-material/Dashboard';
import DownloadIcon from '@mui/icons-material/Download';
import UploadIcon from '@mui/icons-material/UploadFile';
import {
  Alert,
  Button,
  ButtonGroup,
  Popover,
  type PopoverPosition,
  type PopoverProps,
  Snackbar,
  Tooltip,
  styled,
} from '@mui/material';
import {
  Panel,
  ReactFlow,
  type ReactFlowInstance,
  addEdge,
  applyEdgeChanges,
  applyNodeChanges,
  reconnectEdge,
} from '@xyflow/react';
import { inflateSync, strFromU8 } from 'fflate';
import React, { type JSX, useEffect } from 'react';
import AddOperation from './add-operation';
import { EDGE_TYPES } from './edges';
import ExportDiagramDialog from './export-diagram-dialog';
import { EditEdgeForm, EditNodeForm, edgeHasEditForm } from './forms';
import { NODE_TYPES, START_ID, isOperationNode } from './nodes';
import type {
  DiagramEditorEdge,
  DiagramEditorNode,
  OperationNode,
} from './types';
import { autoLayout } from './utils/auto-layout';
import { loadDiagramJson, loadEmpty } from './utils/load-diagram';

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

const NonCapturingPopoverContainer = ({
  children,
}: { children: React.ReactNode }) => <>{children}</>;

const DiagramEditor = () => {
  const reactFlowInstance = React.useRef<ReactFlowInstance<
    DiagramEditorNode,
    DiagramEditorEdge
  > | null>(null);

  const [nodes, setNodes] = React.useState<DiagramEditorNode[]>(
    () => loadEmpty().nodes,
  );
  const [edges, setEdges] = React.useState<DiagramEditorEdge[]>([]);

  const [openAddOpPopover, setOpenAddOpPopover] = React.useState(false);
  const [addOpAnchorPos, setAddOpAnchorPos] = React.useState<PopoverPosition>({
    left: 0,
    top: 0,
  });

  const [editOpFormPopoverProps, setEditOpFormPopoverProps] = React.useState<
    Pick<
      PopoverProps,
      'open' | 'anchorReference' | 'anchorEl' | 'anchorPosition'
    >
  >({ open: false });
  const [renderEditForm, setRenderEditForm] =
    React.useState<JSX.Element | null>(null);

  const closeAllPopovers = React.useCallback(() => {
    setOpenAddOpPopover(false);
    setEditOpFormPopoverProps({ open: false });
  }, []);

  const mouseDownTime = React.useRef(0);

  const loadDiagram = React.useCallback(
    (jsonStr: string) => {
      const graph = loadDiagramJson(jsonStr);
      const changes = autoLayout(START_ID, graph.nodes, graph.edges);
      setNodes(applyNodeChanges(changes, graph.nodes));
      setEdges(graph.edges);
      reactFlowInstance.current?.fitView();
      closeAllPopovers();
    },
    [closeAllPopovers],
  );

  useEffect(() => {
    const queryParams = new URLSearchParams(window.location.search);
    const diagramParam = queryParams.get('diagram');

    if (!diagramParam) {
      return;
    }

    try {
      const binaryString = atob(diagramParam);
      const byteArray = new Uint8Array(binaryString.length);
      for (let i = 0; i < binaryString.length; i++) {
        byteArray[i] = binaryString.charCodeAt(i);
      }
      const diagramJson = strFromU8(inflateSync(byteArray));
      loadDiagram(diagramJson);
    } catch (e) {
      if (e instanceof Error) {
        setErrorToast(`failed to load diagram: ${e.message}`);
        setOpenErrorToast(true);
      } else {
        throw e;
      }
    }
  }, [loadDiagram]);

  const [errorToast, setErrorToast] = React.useState<string | null>(null);
  const [openErrorToast, setOpenErrorToast] = React.useState(false);
  const [openExportDiagramDialog, setOpenExportDiagramDialog] =
    React.useState(false);

  const handleMouseDown = React.useCallback(() => {
    mouseDownTime.current = new Date().getTime();
  }, []);

  return (
    <>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeOrigin={[0.5, 0.5]}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        nodeTypes={NODE_TYPES}
        edgeTypes={EDGE_TYPES}
        onInit={(instance) => {
          reactFlowInstance.current = instance;
        }}
        onNodesChange={(changes) =>
          setNodes((prev) => applyNodeChanges(changes, prev))
        }
        onNodesDelete={() => {
          closeAllPopovers();
        }}
        onEdgesChange={(changes) => {
          setEdges((prev) => applyEdgeChanges(changes, prev));
        }}
        onEdgesDelete={() => {
          closeAllPopovers();
        }}
        onConnect={(conn) => {
          setEdges((prev) => addEdge(conn, prev));
        }}
        onReconnect={(oldEdge, newConnection) =>
          setEdges((prev) => reconnectEdge(oldEdge, newConnection, prev))
        }
        onNodeClick={(ev, node) => {
          ev.stopPropagation();
          closeAllPopovers();

          if (!isOperationNode(node)) {
            return;
          }

          setRenderEditForm(
            <EditNodeForm
              node={node}
              onChange={(change) => {
                setNodes((prev) => applyNodeChanges([change], prev));
              }}
              onDelete={(change) => {
                setNodes((prev) => applyNodeChanges([change], prev));
                closeAllPopovers();
              }}
            />,
          );
          setEditOpFormPopoverProps({
            open: true,
            anchorReference: 'anchorEl',
            anchorEl: ev.currentTarget,
          });
        }}
        onEdgeClick={(ev, edge) => {
          ev.stopPropagation();
          closeAllPopovers();
          if (edgeHasEditForm(edge)) {
            setRenderEditForm(
              <EditEdgeForm
                edge={edge}
                onChange={(change) => {
                  setEdges((prev) => applyEdgeChanges([change], prev));
                }}
              />,
            );
            setEditOpFormPopoverProps({
              open: true,
              anchorReference: 'anchorPosition',
              anchorPosition: { left: ev.clientX, top: ev.clientY },
            });
          }
        }}
        onPaneClick={(ev) => {
          if (openAddOpPopover || editOpFormPopoverProps.open) {
            closeAllPopovers();
            return;
          }

          // filter out erroneous click after connecting an edge
          const now = new Date().getTime();
          if (now - mouseDownTime.current > 200) {
            return;
          }
          setAddOpAnchorPos({ left: ev.clientX, top: ev.clientY });
          setOpenAddOpPopover(true);
        }}
        onMouseDownCapture={handleMouseDown}
        onTouchStartCapture={handleMouseDown}
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
                    const changes = autoLayout(startEdge.target, nodes, edges, {
                      rootPosition: startNode.position,
                    });
                    setNodes((prev) => applyNodeChanges(changes, prev));
                  }
                }}
              >
                <AutoLayoutIcon />
              </Button>
            </Tooltip>
            <Tooltip title="Export Diagram">
              <Button
                onClick={() => {
                  setOpenExportDiagramDialog(true);
                }}
              >
                <DownloadIcon />
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
                      loadDiagram(await ev.target.files[0].text());
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
        // use a custom component to prevent the popover from creating an invisible element that blocks clicks
        component={NonCapturingPopoverContainer}
      >
        <AddOperation
          onAdd={(change) => {
            const newNode = change.item;
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
      <Popover
        {...editOpFormPopoverProps}
        onClose={() => setEditOpFormPopoverProps({ open: false })}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'left' }}
        // use a custom component to prevent the popover from creating an invisible element that blocks clicks
        component={NonCapturingPopoverContainer}
      >
        {renderEditForm}
      </Popover>
      <Snackbar
        open={openErrorToast}
        onClose={(_, reason) => {
          if (reason === 'clickaway') {
            return;
          }
          setOpenErrorToast(false);
        }}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
      >
        <Alert onClose={() => setOpenErrorToast(false)} severity="error">
          {errorToast}
        </Alert>
      </Snackbar>
      <ExportDiagramDialog
        open={openExportDiagramDialog}
        onClose={() => setOpenExportDiagramDialog(false)}
        nodes={nodes}
        edges={edges}
      />
    </>
  );
};

export default DiagramEditor;
