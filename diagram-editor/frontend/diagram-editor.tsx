import {
  Alert,
  alpha,
  darken,
  Fab,
  Popover,
  type PopoverPosition,
  type PopoverProps,
  Snackbar,
  Typography,
  useTheme,
} from '@mui/material';
import {
  addEdge,
  applyEdgeChanges,
  applyNodeChanges,
  Background,
  type Connection,
  type EdgeChange,
  type EdgeRemoveChange,
  type NodeChange,
  type NodeRemoveChange,
  Panel,
  ReactFlow,
  type ReactFlowInstance,
  reconnectEdge,
  type XYPosition,
} from '@xyflow/react';
import { inflateSync, strFromU8 } from 'fflate';
import React from 'react';
import AddOperation from './add-operation';
import CommandPanel from './command-panel';
import type { DiagramEditorEdge } from './edges';
import {
  createBaseEdge,
  EDGE_CATEGORIES,
  EDGE_TYPES,
  EdgeCategory,
} from './edges';
import {
  EditorMode,
  type EditorModeContext,
  EditorModeProvider,
  type UseEditorModeContext,
} from './editor-mode';
import ExportDiagramDialog from './export-diagram-dialog';
import { defaultEdgeData, EditEdgeForm, EditNodeForm } from './forms';
import EditScopeForm from './forms/edit-scope-form';
import type { HandleId } from './handles';
import { NodeManager, NodeManagerProvider } from './node-manager';
import {
  type DiagramEditorNode,
  isBuiltinNode,
  MaterialSymbol,
  NODE_TYPES,
  type OperationNode,
} from './nodes';
import { useTemplates } from './templates-provider';
import { EdgesProvider } from './use-edges';
import { autoLayout } from './utils/auto-layout';
import { isRemoveChange } from './utils/change';
import { getValidEdgeTypes, validateEdgeSimple } from './utils/connection';
import { exhaustiveCheck } from './utils/exhaustive-check';
import { exportTemplate } from './utils/export-diagram';
import { calculateScopeBounds, LAYOUT_OPTIONS } from './utils/layout';
import { loadDiagramJson, loadEmpty, loadTemplate } from './utils/load-diagram';

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
  nodeManager: NodeManager,
  change: NodeChange<DiagramEditorNode>,
): [string, string | null, XYPosition] | null {
  switch (change.type) {
    case 'position': {
      const changedNode = nodeManager.tryGetNode(change.id);
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

interface ProvidersProps {
  editorModeContext: UseEditorModeContext;
  nodeManager: NodeManager;
  edges: DiagramEditorEdge[];
}

function Providers({
  editorModeContext,
  nodeManager,
  edges,
  children,
}: React.PropsWithChildren<ProvidersProps>) {
  return (
    <EditorModeProvider value={editorModeContext}>
      <NodeManagerProvider value={nodeManager}>
        <EdgesProvider value={edges}>{children}</EdgesProvider>
      </NodeManagerProvider>
    </EditorModeProvider>
  );
}

function DiagramEditor() {
  const reactFlowInstance = React.useRef<ReactFlowInstance<
    DiagramEditorNode,
    DiagramEditorEdge
  > | null>(null);

  const [editorMode, setEditorMode] = React.useState<EditorModeContext>({
    mode: EditorMode.Normal,
  });

  const [nodes, setNodes] = React.useState<DiagramEditorNode[]>(
    () => loadEmpty().nodes,
  );
  const nodeManager = React.useMemo(() => new NodeManager(nodes), [nodes]);
  const savedNodes = React.useRef<DiagramEditorNode[]>([]);

  const [edges, setEdges] = React.useState<DiagramEditorEdge[]>([]);
  const savedEdges = React.useRef<DiagramEditorEdge[]>([]);

  const [templates] = useTemplates();

  const updateEditorModeAction = React.useCallback(
    (newMode: EditorModeContext) => {
      switch (newMode.mode) {
        case EditorMode.Normal: {
          setNodes([...savedNodes.current]);
          setEdges([...savedEdges.current]);
          reactFlowInstance.current?.fitView();
          break;
        }
        case EditorMode.Template: {
          const template = templates[newMode.templateId];
          if (!template) {
            throw new Error(`template ${newMode.templateId} not found`);
          }
          const graph = loadTemplate(template);
          const changes = autoLayout(graph.nodes, graph.edges, LAYOUT_OPTIONS);
          // using callback form so that `nodes` and `edges` don't need to be part of dependencies.
          setNodes((prev) => {
            savedNodes.current = [...prev];
            return applyNodeChanges(changes, graph.nodes);
          });
          setEdges((prev) => {
            savedEdges.current = [...prev];
            return graph.edges;
          });
          reactFlowInstance.current?.fitView();
          break;
        }
        default: {
          exhaustiveCheck(newMode);
          throw new Error('unknown editor mode');
        }
      }

      setEditorMode(newMode);
    },
    [templates],
  );

  const handleEdgeChanges = React.useCallback(
    (changes: EdgeChange<DiagramEditorEdge>[]) => {
      setEdges((prev) => applyEdgeChanges(changes, prev));
    },
    [],
  );

  const handleEdgeChange = React.useCallback(
    (change: EdgeChange<DiagramEditorEdge>) => {
      handleEdgeChanges([change]);
    },
    [handleEdgeChanges],
  );

  const [_, setTemplates] = useTemplates();

  const theme = useTheme();

  const backgroundColor = React.useMemo(() => {
    switch (editorMode.mode) {
      case EditorMode.Normal:
        return theme.palette.background.default;
      case EditorMode.Template:
        return darken(theme.palette.primary.main, 0.8);
      default:
        exhaustiveCheck(editorMode);
        throw new Error('unknown editor mode');
    }
  }, [editorMode, theme]);

  const handleNodeChanges = React.useCallback(
    (changes: NodeChange<DiagramEditorNode>[]) => {
      const transitiveChanges: NodeChange<DiagramEditorNode>[] = [];

      // resize and reposition scope
      for (const change of changes) {
        const changeIdPos = getChangeParentIdAndPosition(nodeManager, change);
        if (!changeIdPos) {
          continue;
        }
        const [changeId, changeParentId, changePosition] = changeIdPos;
        if (!changeParentId) {
          continue;
        }

        const scopeNode = nodeManager.tryGetNode(changeParentId);
        if (!scopeNode) {
          continue;
        }
        const scopeChildren = nodeManager.nodes.filter(
          (n) => n.parentId === changeParentId && n.id !== changeId,
        );
        const calculatedBounds = calculateScopeBounds([
          ...scopeChildren.map((n) => n.position),
          {
            // react flow does some kind of rounding (or maybe it is due to floating point accuracies)
            // that results in gitches when resizing a scope quickly. This rounding reduces the
            // impact of the glitches.
            x: Math.round(changePosition.x),
            y: Math.round(changePosition.y),
          },
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
          transitiveChanges.push({
            type: 'position',
            id: changeParentId,
            position: {
              x: newScopeBounds.x,
              y: newScopeBounds.y,
            },
          });
          transitiveChanges.push({
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
            transitiveChanges.push({
              type: 'position',
              id: child.id,
              position: {
                x: child.position.x - calculatedBounds.x,
                y: child.position.y - calculatedBounds.y,
              },
            });
          }
        }
      }

      // remove children nodes of a removed parent
      const removedNodes = new Set(
        changes
          .filter((change) => isRemoveChange(change))
          .map((change) => change.id),
      );
      while (true) {
        let newChanges = false;
        for (const node of nodeManager.nodes) {
          if (
            node.parentId &&
            removedNodes.has(node.parentId) &&
            !removedNodes.has(node.id)
          ) {
            transitiveChanges.push({
              type: 'remove',
              id: node.id,
            });
            removedNodes.add(node.id);
            newChanges = true;
          }
        }
        if (!newChanges) {
          break;
        }
      }

      // clean up dangling edges when a node is removed.
      const edgeChanges: EdgeRemoveChange[] = [];
      for (const edge of edges) {
        if (removedNodes.has(edge.source) || removedNodes.has(edge.target)) {
          edgeChanges.push({
            type: 'remove',
            id: edge.id,
          });
        }
      }
      handleEdgeChanges(edgeChanges);

      setNodes((prev) =>
        applyNodeChanges([...changes, ...transitiveChanges], prev),
      );
    },
    [handleEdgeChanges, nodeManager, edges],
  );

  const handleNodeChange = React.useCallback(
    (change: NodeChange<DiagramEditorNode>) => {
      handleNodeChanges([change]);
    },
    [handleNodeChanges],
  );

  const [addOperationPopover, setAddOperationPopover] = React.useState<{
    open: boolean;
    popOverPosition: PopoverPosition;
    parentId: string | null;
  }>({
    open: false,
    popOverPosition: { left: 0, top: 0 },
    parentId: null,
  });
  const addOperationNewNodePosition = React.useMemo<XYPosition>(() => {
    if (!reactFlowInstance.current) {
      return { x: 0, y: 0 };
    }
    const parentNode = addOperationPopover.parentId
      ? nodeManager.tryGetNode(addOperationPopover.parentId)
      : null;

    const parentPosition = parentNode?.position
      ? parentNode.position
      : { x: 0, y: 0 };
    return (
      reactFlowInstance.current?.screenToFlowPosition({
        x: addOperationPopover.popOverPosition.left - parentPosition.x,
        y: addOperationPopover.popOverPosition.top - parentPosition.y,
      }) || { x: 0, y: 0 }
    );
  }, [
    nodeManager,
    addOperationPopover.parentId,
    addOperationPopover.popOverPosition,
  ]);

  const [editingNodeId, setEditingNodeId] = React.useState<string | null>(null);

  const [editingEdgeId, setEditingEdgeId] = React.useState<string | null>(null);
  const editingEdge: EditingEdge | null = (() => {
    if (!editingEdgeId) {
      return null;
    }

    const edge = edges.find((e) => e.id === editingEdgeId);
    if (!edge) {
      console.error(`cannot find edge ${editingEdgeId}`);
      return null;
    }

    const sourceNode = nodeManager.tryGetNode(edge.source);
    if (!sourceNode) {
      console.error(`cannot find node ${edge.source}`);
      return null;
    }
    const targetNode = nodeManager.tryGetNode(edge.target);
    if (!targetNode) {
      console.error(`cannot find node ${edge.target}`);
      return null;
    }

    return {
      edge,
      sourceNode,
      targetNode,
    };
  })();

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
      const node = nodeManager.tryGetNode(nodeId);
      if (!node || isBuiltinNode(node)) {
        return null;
      }

      const handleDelete = (change: NodeRemoveChange) => {
        handleNodeChange(change);
        closeAllPopovers();
      };

      if (node.type === 'scope') {
        return (
          <EditScopeForm
            node={node as OperationNode<'scope'>}
            onChange={handleNodeChange}
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
          key={editingNodeId}
          node={node}
          onChange={handleNodeChange}
          onDelete={handleDelete}
        />
      );
    },
    [nodeManager, editingNodeId, handleNodeChange, closeAllPopovers],
  );

  const mouseDownTime = React.useRef(0);

  const [errorToast, setErrorToast] = React.useState<string | null>(null);
  const [openErrorToast, setOpenErrorToast] = React.useState(false);
  const showErrorToast = React.useCallback((message: string) => {
    setErrorToast(message);
    setOpenErrorToast(true);
  }, []);

  const loadDiagram = React.useCallback(
    (jsonStr: string) => {
      try {
        const [diagram, graph] = loadDiagramJson(jsonStr);
        const changes = autoLayout(graph.nodes, graph.edges, LAYOUT_OPTIONS);
        setNodes(applyNodeChanges(changes, graph.nodes));
        setEdges(graph.edges);
        setTemplates(diagram.templates || {});
        reactFlowInstance.current?.fitView();
        closeAllPopovers();
      } catch (e) {
        showErrorToast(`failed to load diagram: ${e}`);
      }
    },
    [closeAllPopovers, showErrorToast],
  );

  const [openExportDiagramDialog, setOpenExportDiagramDialog] =
    React.useState(false);

  const handleMouseDown = React.useCallback(() => {
    mouseDownTime.current = Date.now();
  }, []);

  const tryCreateEdge = React.useCallback(
    (conn: Connection, id?: string): DiagramEditorEdge | null => {
      const sourceNode = nodeManager.tryGetNode(conn.source);
      const targetNode = nodeManager.tryGetNode(conn.target);
      if (!sourceNode || !targetNode) {
        throw new Error('cannot find source or target node');
      }

      const validEdges = getValidEdgeTypes(
        sourceNode,
        conn.sourceHandle as HandleId,
        targetNode,
        conn.targetHandle as HandleId,
      );
      if (validEdges.length === 0) {
        showErrorToast(
          `cannot connect "${sourceNode.type}" to "${targetNode.type}"`,
        );
        return null;
      }

      const newEdge = {
        ...createBaseEdge(conn.source, conn.target, id),
        type: validEdges[0],
        data: defaultEdgeData(validEdges[0]),
      } as DiagramEditorEdge;

      if (targetNode.type === 'section') {
        if (EDGE_CATEGORIES[newEdge.type] === EdgeCategory.Buffer) {
          newEdge.data.input = { type: 'sectionBuffer', inputId: '' };
        } else if (EDGE_CATEGORIES[newEdge.type] === EdgeCategory.Data) {
          newEdge.data.input = { type: 'sectionInput', inputId: '' };
        }
      }

      const validationResult = validateEdgeSimple(newEdge, nodeManager, edges);
      if (!validationResult.valid) {
        showErrorToast(validationResult.error);
        return null;
      }

      return newEdge;
    },
    [showErrorToast, nodeManager, edges],
  );

  return (
    <Providers
      editorModeContext={[editorMode, updateEditorModeAction]}
      nodeManager={nodeManager}
      edges={edges}
    >
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
          const newEdge = tryCreateEdge(conn);
          if (newEdge) {
            setEdges((prev) => addEdge(newEdge, prev));
          }
        }}
        isValidConnection={(conn) => {
          const sourceNode = nodeManager.tryGetNode(conn.source);
          const targetNode = nodeManager.tryGetNode(conn.target);
          if (!sourceNode || !targetNode) {
            throw new Error('cannot find source or target node');
          }

          const allowedEdges = getValidEdgeTypes(
            sourceNode,
            conn.sourceHandle as HandleId,
            targetNode,
            conn.targetHandle as HandleId,
          );
          return allowedEdges.length > 0;
        }}
        onReconnect={(oldEdge, newConnection) => {
          const newEdge = tryCreateEdge(newConnection, oldEdge.id);
          if (newEdge) {
            oldEdge.type = newEdge.type;
            oldEdge.data = newEdge.data;
            setEdges((prev) => reconnectEdge(oldEdge, newConnection, prev));
          }
        }}
        onNodeClick={(ev, node) => {
          ev.stopPropagation();
          closeAllPopovers();

          if (isBuiltinNode(node)) {
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
        <Background
          bgColor={backgroundColor}
          color={alpha(theme.palette.text.primary, 0.3)}
          gap={30}
        />
        {editorMode.mode === EditorMode.Template && (
          <Panel position="top-left">
            <Typography variant="h4">{editorMode.templateId}</Typography>
          </Panel>
        )}
        <CommandPanel
          onNodeChanges={handleNodeChanges}
          onExportClick={React.useCallback(
            () => setOpenExportDiagramDialog(true),
            [],
          )}
          onLoadDiagram={loadDiagram}
        />
        {editorMode.mode === EditorMode.Template && (
          <Fab
            color="primary"
            aria-label="Save"
            sx={{ position: 'absolute', right: 64, bottom: 64 }}
            onClick={() => {
              const exportedTemplate = exportTemplate(nodeManager, edges);
              setTemplates((prev) => ({
                ...prev,
                [editorMode.templateId]: exportedTemplate,
              }));
              updateEditorModeAction({ mode: EditorMode.Normal });
            }}
          >
            <MaterialSymbol symbol="check" />
          </Fab>
        )}
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
            parentId={addOperationPopover.parentId || undefined}
            newNodePosition={addOperationNewNodePosition}
            onAdd={(changes) => {
              handleNodeChanges(changes);
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
              key={editingEdgeId}
              edge={editingEdge.edge}
              allowedEdgeTypes={getValidEdgeTypes(
                editingEdge.sourceNode,
                editingEdge.edge.sourceHandle,
                editingEdge.targetNode,
                editingEdge.edge.targetHandle,
              )}
              onChange={handleEdgeChange}
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
        />
      </ReactFlow>
    </Providers>
  );
}

export default DiagramEditor;
