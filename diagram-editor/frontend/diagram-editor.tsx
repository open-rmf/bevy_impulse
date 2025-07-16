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
  Background,
  type EdgeChange,
  type EdgeRemoveChange,
  type NodeChange,
  type NodeRemoveChange,
  ReactFlow,
  type ReactFlowInstance,
  reconnectEdge,
  type XYPosition,
} from '@xyflow/react';
import { inflateSync, strFromU8 } from 'fflate';
import React from 'react';
import AddOperation from './add-operation';
import CommandPanel from './command-panel';
import { EDGE_TYPES } from './edges';
import ExportDiagramDialog from './export-diagram-dialog';
import { defaultEdgeData, EditEdgeForm, EditNodeForm } from './forms';
import EditScopeForm from './forms/edit-scope-form';
import { NODE_TYPES } from './nodes';
import type {
  DiagramEditorEdge,
  DiagramEditorNode,
  OperationNode,
} from './types';
import { allowEdges as getAllowEdges, isOperationNode } from './utils';
import { autoLayout } from './utils/auto-layout';
import { calculateScopeBounds, LAYOUT_OPTIONS } from './utils/layout';
import { loadDiagramJson, loadEmpty } from './utils/load-diagram';

const NonCapturingPopoverContainer = ({
  children,
}: {
  children: React.ReactNode;
}) => <>{children}</>;

interface EditingEdge {
  sourceNode: DiagramEditorNode;
  targetNode: DiagramEditorNode;
  edge: DiagramEditorEdge;
}

/**
 * Given a node change, get the parent id and the new position if the change would be applied.
 * Returns null if the change does not result in any position changes.
 */
function getChangeParentIdAndPosition(
  reactFlow: ReactFlowInstance<DiagramEditorNode, DiagramEditorEdge>,
  change: NodeChange<DiagramEditorNode>,
): [string, string | null, XYPosition] | null {
  switch (change.type) {
    case 'position': {
      const changedNode = reactFlow.getNode(change.id);
      if (!changedNode) {
        return null;
      }
      return change.position
        ? [change.id, changedNode.parentId || null, change.position]
        : null;
    }
    case 'add':
    case 'replace': {
      return [
        change.item.id,
        change.item.parentId || null,
        change.item.position,
      ];
    }
    default: {
      return null;
    }
  }
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
  const handleEdgeChanges = React.useCallback(
    (changes: EdgeChange<DiagramEditorEdge>[]) => {
      setEdges((prev) => applyEdgeChanges(changes, prev));
    },
    [],
  );

  const handleNodeChanges = React.useCallback(
    (changes: NodeChange<DiagramEditorNode>[]) => {
      const reactFlow = reactFlowInstance.current;
      if (!reactFlow) {
        return;
      }

      setNodes((prev) => {
        const scopeChanges: NodeChange<DiagramEditorNode>[] = [];
        for (const change of changes) {
          const changeIdPos = getChangeParentIdAndPosition(reactFlow, change);
          if (!changeIdPos) {
            continue;
          }
          const [changeId, changeParentId, changePosition] = changeIdPos;
          if (!changeParentId) {
            continue;
          }

          const scopeNode = prev.find((n) => n.id === changeParentId);
          if (!scopeNode) {
            continue;
          }
          const scopeChildren = prev.filter(
            (n) => n.parentId === changeParentId && n.id !== changeId,
          );
          const calculatedBounds = calculateScopeBounds([
            ...scopeChildren.map((n) => n.position),
            changePosition,
          ]);

          const newScopeBounds = {
            x: scopeNode.position.x,
            y: scopeNode.position.y,
            width: scopeNode.width ?? calculatedBounds.width,
            height: scopeNode.height ?? calculatedBounds.height,
          };
          // React Flow cannot handle fast changing of a parent's position while changing the
          // children's position as well (some kind of race condition that causes the node positions to jump around).
          // Workaround by resizing the scope only if it hits a certain threshold.
          if (
            Math.abs(calculatedBounds.x) >
              LAYOUT_OPTIONS.scopePadding.leftRight ||
            Math.abs(calculatedBounds.y) >
              LAYOUT_OPTIONS.scopePadding.topBottom ||
            (scopeNode.width &&
              Math.abs(calculatedBounds.width - scopeNode.width) >
                LAYOUT_OPTIONS.scopePadding.leftRight) ||
            (scopeNode.height &&
              Math.abs(calculatedBounds.height - scopeNode.height) >
                LAYOUT_OPTIONS.scopePadding.topBottom)
          ) {
            newScopeBounds.x += calculatedBounds.x;
            newScopeBounds.width = calculatedBounds.width;
            newScopeBounds.y += calculatedBounds.y;
            newScopeBounds.height = calculatedBounds.height;
          }

          if (
            newScopeBounds.x !== scopeNode.position.x ||
            newScopeBounds.y !== scopeNode.position.y ||
            newScopeBounds.width !== scopeNode.width ||
            newScopeBounds.height !== scopeNode.height
          ) {
            scopeChanges.push({
              type: 'position',
              id: changeParentId,
              position: {
                x: newScopeBounds.x,
                y: newScopeBounds.y,
              },
            });
            scopeChanges.push({
              type: 'dimensions',
              id: changeParentId,
              dimensions: {
                width: newScopeBounds.width,
                height: newScopeBounds.height,
              },
              setAttributes: true,
            });
            // when the scope is moved, the relative position of the nodes will change so we
            // need to update them to keep them in place.
            for (const child of scopeChildren) {
              scopeChanges.push({
                type: 'position',
                id: child.id,
                position: {
                  x: child.position.x - calculatedBounds.x,
                  y: child.position.y - calculatedBounds.y,
                },
              });
            }
            changePosition.x -= calculatedBounds.x;
            changePosition.y -= calculatedBounds.y;
          }
        }

        return applyNodeChanges([...changes, ...scopeChanges], prev);
      });

      // clean up dangling edges when a node is removed.
      for (const change of changes) {
        if (change.type !== 'remove') {
          continue;
        }

        const edgeChanges: EdgeRemoveChange[] = [];
        for (const edge of reactFlowInstance.current?.getEdges() || []) {
          if (edge.source === change.id || edge.target === change.id) {
            edgeChanges.push({
              type: 'remove',
              id: edge.id,
            });
          }
        }
        handleEdgeChanges(edgeChanges);
      }
    },
    [handleEdgeChanges],
  );

  const [addOperationPopover, setAddOperationPopover] = React.useState<{
    open: boolean;
    popOverPosition: PopoverPosition;
    parentId: string | null;
  }>({ open: false, popOverPosition: { left: 0, top: 0 }, parentId: null });

  const [editingNodeId, setEditingNodeId] = React.useState<string | null>(null);

  const [editingEdgeId, setEditingEdgeId] = React.useState<string | null>(null);
  const editingEdge = React.useMemo<EditingEdge | null>(() => {
    if (!reactFlowInstance.current || !editingEdgeId) {
      return null;
    }

    const edge = reactFlowInstance.current.getEdge(editingEdgeId);
    if (!edge) {
      console.error(`cannot find edge ${editingEdgeId}`);
      return null;
    }

    const sourceNode = reactFlowInstance.current.getNode(edge.source);
    if (!sourceNode) {
      console.error(`cannot find node ${edge.source}`);
      return null;
    }
    const targetNode = reactFlowInstance.current.getNode(edge.target);
    if (!targetNode) {
      console.error(`cannot find node ${edge.target}`);
      return null;
    }

    return {
      edge,
      sourceNode,
      targetNode,
    };
  }, [editingEdgeId]);

  const closeAllPopovers = React.useCallback(() => {
    setEditingNodeId(null);
    setEditingEdgeId(null);
    setAddOperationPopover((prev) => ({ ...prev, open: false }));
    setEditOpFormPopoverProps({ open: false });
  }, []);

  const [editOpFormPopoverProps, setEditOpFormPopoverProps] = React.useState<
    Pick<
      PopoverProps,
      'open' | 'anchorReference' | 'anchorEl' | 'anchorPosition'
    >
  >({ open: false });
  const renderEditForm = React.useCallback(
    (nodeId: string) => {
      if (!reactFlowInstance.current) {
        return null;
      }
      const node = reactFlowInstance.current.getNode(nodeId);
      if (!node || !isOperationNode(node)) {
        return null;
      }

      const handleDelete = (change: NodeRemoveChange) => {
        handleNodeChanges([change]);
        closeAllPopovers();
      };

      if (node.type === 'scope') {
        return (
          <EditScopeForm
            node={node as OperationNode<'scope'>}
            onChanges={handleNodeChanges}
            onDelete={handleDelete}
            onAddOperationClick={(ev) => {
              setAddOperationPopover({
                open: true,
                popOverPosition: { left: ev.clientX, top: ev.clientY },
                parentId: node.id,
              });
            }}
          />
        );
      }
      return (
        <EditNodeForm
          node={node}
          onChanges={handleNodeChanges}
          onDelete={handleDelete}
        />
      );
    },
    [handleNodeChanges, closeAllPopovers],
  );

  const mouseDownTime = React.useRef(0);

  const loadDiagram = React.useCallback(
    (jsonStr: string) => {
      if (!reactFlowInstance.current) {
        return;
      }

      const graph = loadDiagramJson(jsonStr);
      const changes = autoLayout(graph.nodes, graph.edges, LAYOUT_OPTIONS);
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
        fitView
        fitViewOptions={{ padding: 0.2 }}
        nodeTypes={NODE_TYPES}
        edgeTypes={EDGE_TYPES}
        onInit={(instance) => {
          reactFlowInstance.current = instance;

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
        }}
        onNodesChange={handleNodeChanges}
        onNodesDelete={() => {
          closeAllPopovers();
        }}
        onEdgesChange={handleEdgeChanges}
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
          setEditingNodeId(node.id);

          setEditOpFormPopoverProps({
            open: true,
            anchorReference: 'anchorPosition',
            anchorPosition: { left: ev.clientX, top: ev.clientY },
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

          setEditingEdgeId(edge.id);

          setEditOpFormPopoverProps({
            open: true,
            anchorReference: 'anchorPosition',
            anchorPosition: { left: ev.clientX, top: ev.clientY },
          });
        }}
        onPaneClick={(ev) => {
          if (addOperationPopover.open || editOpFormPopoverProps.open) {
            closeAllPopovers();
            return;
          }

          // filter out erroneous click after connecting an edge
          const now = Date.now();
          if (now - mouseDownTime.current > 200) {
            return;
          }
          setAddOperationPopover({
            open: true,
            popOverPosition: { left: ev.clientX, top: ev.clientY },
            parentId: null,
          });
        }}
        onMouseDownCapture={handleMouseDown}
        onTouchStartCapture={handleMouseDown}
        colorMode="dark"
        deleteKeyCode={'Delete'}
      >
        <Background style={{ opacity: 0.5 }} />
        <CommandPanel
          onNodeChanges={handleNodeChanges}
          onExportClick={React.useCallback(
            () => setOpenExportDiagramDialog(true),
            [],
          )}
          onLoadDiagram={loadDiagram}
        />
      </ReactFlow>
      <Popover
        open={addOperationPopover.open}
        onClose={() =>
          setAddOperationPopover((prev) => ({ ...prev, open: false }))
        }
        anchorReference="anchorPosition"
        anchorPosition={addOperationPopover.popOverPosition}
        // use a custom component to prevent the popover from creating an invisible element that blocks clicks
        component={NonCapturingPopoverContainer}
      >
        <AddOperation
          onAdd={(change) => {
            const newNode = change.item;
            newNode.parentId = addOperationPopover.parentId || undefined;
            const newPos = reactFlowInstance.current?.screenToFlowPosition({
              x: addOperationPopover.popOverPosition.left,
              y: addOperationPopover.popOverPosition.top,
            });
            if (!newPos) {
              throw new Error(
                'failed to add operation: cannot determine position',
              );
            }

            const parentNode = newNode.parentId
              ? reactFlowInstance.current?.getNode(newNode.parentId)
              : null;
            const parentPosition = parentNode?.position
              ? parentNode.position
              : { x: 0, y: 0 };
            newPos.x -= parentPosition.x;
            newPos.y -= parentPosition.y;

            newNode.position = newPos;
            handleNodeChanges([change]);
            closeAllPopovers();
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
        {editingNodeId && renderEditForm(editingNodeId)}
        {editingEdge && (
          <EditEdgeForm
            edge={editingEdge.edge}
            allowedEdgeTypes={getAllowEdges(
              editingEdge.sourceNode,
              editingEdge.targetNode,
            )}
            onChanges={handleEdgeChanges}
            onDelete={(changes) => {
              setEdges((prev) => applyEdgeChanges([changes], prev));
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
