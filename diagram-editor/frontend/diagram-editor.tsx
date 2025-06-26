import {
  Alert,
  Popover,
  type PopoverPosition,
  type PopoverProps,
  Snackbar,
} from '@mui/material';
import {
  addEdge,
  applyEdgeChanges,
  applyNodeChanges,
  type EdgeRemoveChange,
  ReactFlow,
  type ReactFlowInstance,
  reconnectEdge,
} from '@xyflow/react';
import { inflateSync, strFromU8 } from 'fflate';
import React, { useEffect } from 'react';
import AddOperation from './add-operation';
import CommandPanel from './command-panel';
import { EDGE_TYPES } from './edges';
import ExportDiagramDialog from './export-diagram-dialog';
import { defaultEdgeData, EditEdgeForm, EditNodeForm } from './forms';
import { NODE_TYPES, START_ID } from './nodes';
import type {
  DiagramEditorEdge,
  DiagramEditorNode,
  OperationNode,
} from './types';
import { allowEdges as getAllowEdges, isOperationNode } from './utils';
import { autoLayout } from './utils/auto-layout';
import { loadDiagramJson, loadEmpty } from './utils/load-diagram';

const NonCapturingPopoverContainer = ({
  children,
}: {
  children: React.ReactNode;
}) => <>{children}</>;

interface SelectedEdge {
  sourceNode: DiagramEditorNode;
  targetNode: DiagramEditorNode;
  edge: DiagramEditorEdge;
}

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

  const [selectedNodeId, setSelectedNodeId] = React.useState<string | null>(
    null,
  );
  const selectedNode = React.useMemo<OperationNode | null>(() => {
    if (!selectedNodeId) {
      return null;
    }
    const node = nodes.find((n) => n.id === selectedNodeId);
    if (!node) {
      console.error(`cannot find node ${selectedNodeId}`);
      return null;
    }
    if (!isOperationNode(node)) {
      return null;
    }
    return node;
  }, [selectedNodeId, nodes]);

  const [selectedEdgeId, setSelectedEdgeId] = React.useState<string | null>(
    null,
  );
  const selectedEdge = React.useMemo<SelectedEdge | null>(() => {
    if (!selectedEdgeId) {
      return null;
    }

    const edge = edges.find((e) => e.id === selectedEdgeId);
    if (!edge) {
      console.error(`cannot find edge ${selectedEdgeId}`);
      return null;
    }

    const sourceNode = nodes.find((n) => n.id === edge.source);
    if (!sourceNode) {
      console.error(`cannot find node ${edge.source}`);
      return null;
    }
    const targetNode = nodes.find((n) => n.id === edge.target);
    if (!targetNode) {
      console.error(`cannot find node ${edge.target}`);
      return null;
    }

    return {
      edge,
      sourceNode,
      targetNode,
    };
  }, [selectedEdgeId, nodes, edges]);

  const closeAllPopovers = React.useCallback(() => {
    setSelectedNodeId(null);
    setSelectedEdgeId(null);
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

  const [errorToast, setErrorToast] = React.useState<string | null>(null);
  const [openErrorToast, setOpenErrorToast] = React.useState(false);
  const showErrorToast = React.useCallback((message: string) => {
    setErrorToast(message);
    setOpenErrorToast(true);
  }, []);

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
        showErrorToast(`failed to load diagram: ${e.message}`);
      } else {
        throw e;
      }
    }
  }, [loadDiagram, showErrorToast]);

  const [openExportDiagramDialog, setOpenExportDiagramDialog] =
    React.useState(false);

  const handleMouseDown = React.useCallback(() => {
    mouseDownTime.current = Date.now();
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
        onNodesChange={(changes) => {
          setNodes((prev) => applyNodeChanges(changes, prev));
        }}
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
          const sourceNode = nodes.find((n) => n.id === conn.source);
          const targetNode = nodes.find((n) => n.id === conn.target);
          if (!sourceNode || !targetNode) {
            throw new Error('cannot find source or target node');
          }

          const allowedEdges = getAllowEdges(sourceNode, targetNode);
          if (allowedEdges.length === 0) {
            showErrorToast(
              `cannot connect "${sourceNode.type}" to "${targetNode.type}"`,
            );
            return;
          }

          setEdges((prev) =>
            addEdge(
              {
                ...conn,
                type: allowedEdges[0],
                data: defaultEdgeData(allowedEdges[0]),
              },
              prev,
            ),
          );
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
          setSelectedNodeId(node.id);

          setEditOpFormPopoverProps({
            open: true,
            anchorReference: 'anchorEl',
            anchorEl: ev.currentTarget,
          });
        }}
        onEdgeClick={(ev, edge) => {
          ev.stopPropagation();
          closeAllPopovers();

          const sourceNode = nodes.find((n) => n.id === edge.source);
          const targetNode = nodes.find((n) => n.id === edge.target);
          if (!sourceNode || !targetNode) {
            throw new Error('unable to find source or target node');
          }

          setSelectedEdgeId(edge.id);

          setEditOpFormPopoverProps({
            open: true,
            anchorReference: 'anchorPosition',
            anchorPosition: { left: ev.clientX, top: ev.clientY },
          });
        }}
        onPaneClick={(ev) => {
          if (openAddOpPopover || editOpFormPopoverProps.open) {
            closeAllPopovers();
            return;
          }

          // filter out erroneous click after connecting an edge
          const now = Date.now();
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
        <CommandPanel
          onNodeChanges={React.useCallback(
            (changes) => setNodes((prev) => applyNodeChanges(changes, prev)),
            [],
          )}
          onExportClick={React.useCallback(
            () => setOpenExportDiagramDialog(true),
            [],
          )}
          onLoadDiagram={loadDiagram}
        />
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
        {selectedNode && (
          <EditNodeForm
            node={selectedNode}
            onChange={(change) => {
              setNodes((prev) => applyNodeChanges([change], prev));
            }}
            onDelete={(change) => {
              setNodes((prev) => applyNodeChanges([change], prev));
              const edgeChanges: EdgeRemoveChange[] = [];
              for (const edge of edges) {
                if (edge.source === change.id || edge.target === change.id) {
                  edgeChanges.push({
                    type: 'remove',
                    id: edge.id,
                  });
                }
              }
              setEdges((prev) => applyEdgeChanges(edgeChanges, prev));
              closeAllPopovers();
            }}
          />
        )}
        {selectedEdge && (
          <EditEdgeForm
            edge={selectedEdge.edge}
            allowedEdgeTypes={getAllowEdges(
              selectedEdge.sourceNode,
              selectedEdge.targetNode,
            )}
            onChange={(change) => {
              setEdges((prev) => applyEdgeChanges([change], prev));
            }}
            onDelete={(change) => {
              setEdges((prev) => applyEdgeChanges([change], prev));
              closeAllPopovers();
            }}
          />
        )}
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
