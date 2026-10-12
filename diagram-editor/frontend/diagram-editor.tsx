import {
  Alert,
  alpha,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  darken,
  Fab,
  Popover,
  type PopoverPosition,
  type PopoverProps,
  Snackbar,
  Typography,
  useTheme,
} from '@mui/material';
import type { PopoverActions } from '@mui/material/Popover';
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
import React, { Suspense } from 'react';
import AddOperation from './add-operation';
import CommandPanel from './command-panel';
import { CompatibleAddOperation } from './compatible-add-operation';
import {
  ConnectionCompatibilityProvider,
  useCompatibilityGraph,
} from './connection-compatibility-provider';
import { ConnectionHintPanel } from './connection-hint-panel';
import {
  createEmptyDiagramProperties,
  useDiagramProperties,
} from './diagram-properties-provider';
import { useDiagramSidePanel } from './diagram-side-panel-controller';
import { getEditPopoverPositionForNode } from './diagram-side-panel-layout';
import {
  clearDraftWorkspace,
  type DraftWorkspaceContent,
  type DraftWorkspaceV1,
  draftWorkspaceFingerprint,
  readDraftWorkspace,
  writeDraftWorkspace,
} from './draft-workspace';
import type { DiagramEditorEdge } from './edges';
import { EDGE_TYPES } from './edges';
import {
  EditorMode,
  type EditorModeContext,
  EditorModeProvider,
  type UseEditorModeContext,
} from './editor-mode';
import { ExportDiagramDialog } from './export-diagram-dialog';
import { EditEdgeForm, EditNodeForm } from './forms';
import EditScopeForm from './forms/edit-scope-form';
import type { ScriptNodeEnvironmentBinding } from './forms/script-environment-workspace';
import { useScriptEnvironmentNavigation } from './forms/use-script-environment-navigation';
import {
  type InteractionVisualizationContext,
  InteractionVisualizationProvider,
} from './interaction-visualization-provider';
import { type LoadContext, LoadContextProvider } from './load-context-provider';
import {
  NewDiagramDialog,
  useNewDiagramAfterExport,
} from './new-diagram-dialog';
import { NodeManager, NodeManagerProvider } from './node-manager';
import {
  type DiagramEditorNode,
  isBuiltinNode,
  MaterialSymbol,
  NODE_TYPES,
  type OperationNode,
  TERMINATE_ID,
} from './nodes';
import { NotificationProvider } from './notification-provider';
import { useRegistry } from './registry-provider';
import { ResponsiveEditPopover } from './responsive-edit-popover';
import { useTemplates } from './templates-provider';
import { useTransientEditorDrafts } from './transient-editor-drafts';
import type { Diagram } from './types/api';
import { useBeforeUnloadWarning } from './use-before-unload-warning';
import { useDraftPagehideFlush } from './use-draft-pagehide-flush';
import { EdgesProvider } from './use-edges';
import { autoLayout } from './utils/auto-layout';
import { isRemoveChange } from './utils/change';
import { prepareConnection } from './utils/compatibility';
import {
  createConnectionFromHandles,
  getValidEdgeTypes,
  validateConnectionSimple,
  validateDraggedHandlePair,
  validateSourceOutputCapacity,
} from './utils/connection';
import { shouldIgnoreEscapeClose } from './utils/editing-target';
import { exhaustiveCheck } from './utils/exhaustive-check';
import { exportTemplate } from './utils/export-diagram';
import { calculateScopeBounds, LAYOUT_OPTIONS } from './utils/layout';
import {
  type LoadedDiagram,
  loadDiagramJson,
  loadEmpty,
  loadTemplate,
} from './utils/load-diagram';
import { joinNamespaces, ROOT_NAMESPACE } from './utils/namespace';

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

interface PreparedDiagram {
  diagram: Diagram;
  loaded: LoadedDiagram;
  filename: string | null;
}

interface RecoveryCandidate {
  draft: DraftWorkspaceV1;
  linkedDiagram: PreparedDiagram | null;
}

function decodeDiagramParam(diagramParam: string): string {
  const binaryString = atob(diagramParam);
  const byteArray = new Uint8Array(binaryString.length);
  for (let i = 0; i < binaryString.length; i++) {
    byteArray[i] = binaryString.charCodeAt(i);
  }
  return strFromU8(inflateSync(byteArray));
}

function cloneSerializable<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function snapshotGraph(
  nodes: DiagramEditorNode[],
  edges: DiagramEditorEdge[],
): { nodes: DiagramEditorNode[]; edges: DiagramEditorEdge[] } {
  return {
    nodes: nodes.map(({ selected: _selected, dragging: _dragging, ...node }) =>
      cloneSerializable(node),
    ) as DiagramEditorNode[],
    edges: edges.map(({ selected: _selected, ...edge }) =>
      cloneSerializable(edge),
    ) as DiagramEditorEdge[],
  };
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

export type MaybeValid = { ok: true } | { ok: false; errorMessage: string };

interface ProvidersProps {
  editorModeContext: UseEditorModeContext;
  interactionVisualizationContext: InteractionVisualizationContext;
  loadContext: LoadContext | null;
  nodeManager: NodeManager;
  edges: DiagramEditorEdge[];
  compatibility: ReturnType<typeof useCompatibilityGraph>;
}

function Providers({
  editorModeContext,
  interactionVisualizationContext,
  loadContext,
  nodeManager,
  edges,
  compatibility,
  children,
}: React.PropsWithChildren<ProvidersProps>) {
  return (
    <EditorModeProvider value={editorModeContext}>
      <LoadContextProvider value={loadContext}>
        <NodeManagerProvider value={nodeManager}>
          <EdgesProvider value={edges}>
            <ConnectionCompatibilityProvider value={compatibility}>
              <InteractionVisualizationProvider
                value={interactionVisualizationContext}
              >
                <NotificationProvider>{children}</NotificationProvider>
              </InteractionVisualizationProvider>
            </ConnectionCompatibilityProvider>
          </EdgesProvider>
        </NodeManagerProvider>
      </LoadContextProvider>
    </EditorModeProvider>
  );
}

function getInteractionNodeId(
  operationId: string,
  nodeManager: NodeManager,
): string | null {
  const normalizedId = operationId.startsWith(':')
    ? operationId.slice(1)
    : operationId;

  if (normalizedId === '(terminate)') {
    return (
      nodeManager.tryGetNode(joinNamespaces(ROOT_NAMESPACE, TERMINATE_ID))
        ?.id ?? null
    );
  }

  if (normalizedId.endsWith(':(terminate)')) {
    const namespace = normalizedId.slice(0, -':(terminate)'.length);
    return (
      nodeManager.tryGetNode(
        joinNamespaces(ROOT_NAMESPACE, namespace, TERMINATE_ID),
      )?.id ?? null
    );
  }

  if (normalizedId.startsWith('(') || normalizedId.includes(':(exposed):')) {
    return null;
  }

  const parts = normalizedId.split(':');
  const opId = parts.at(-1);
  if (!opId) {
    return null;
  }

  try {
    return nodeManager.getNodeFromNamespaceOpId(
      joinNamespaces(ROOT_NAMESPACE, ...parts.slice(0, -1)),
      opId,
    ).id;
  } catch {
    return null;
  }
}

function DiagramEditor() {
  const reactFlowInstance = React.useRef<ReactFlowInstance<
    DiagramEditorNode,
    DiagramEditorEdge
  > | null>(null);
  const suppressNextPaneClick = React.useRef(false);

  const [editorMode, setEditorMode] = React.useState<EditorModeContext>({
    mode: EditorMode.Normal,
  });

  const [nodes, setNodes] = React.useState<DiagramEditorNode[]>(
    () => loadEmpty().nodes,
  );
  const nodeManager = React.useMemo(() => new NodeManager(nodes), [nodes]);
  const [interactionActiveNodeIds, setInteractionActiveNodeIds] =
    React.useState(() => new Set<string>());
  const [interactionVisitedNodeIds, setInteractionVisitedNodeIds] =
    React.useState(() => new Set<string>());
  const [isWorkflowRunning, setWorkflowRunning] = React.useState(false);
  const clearInteractionVisualization = React.useCallback(() => {
    setInteractionActiveNodeIds(new Set());
    setInteractionVisitedNodeIds(new Set());
  }, []);
  const markInteractionFinished = React.useCallback(() => {
    setInteractionActiveNodeIds(new Set());
  }, []);
  const markInteractionOperationFinished = React.useCallback(
    (operationId: string) => {
      const nodeId = getInteractionNodeId(operationId, nodeManager);
      if (!nodeId) {
        return;
      }

      setInteractionActiveNodeIds((prev) => {
        const next = new Set(prev);
        next.delete(nodeId);
        return next;
      });
      setInteractionVisitedNodeIds((prev) => {
        const next = new Set(prev);
        next.add(nodeId);
        return next;
      });
    },
    [nodeManager],
  );
  const markInteractionOperationStarted = React.useCallback(
    (operationId: string) => {
      const nodeId = getInteractionNodeId(operationId, nodeManager);
      if (!nodeId) {
        return;
      }

      setInteractionVisitedNodeIds((prev) => {
        const next = new Set(prev);
        next.delete(nodeId);
        return next;
      });
      setInteractionActiveNodeIds((prev) => {
        const next = new Set(prev);
        next.add(nodeId);
        return next;
      });
    },
    [nodeManager],
  );
  const interactionVisualizationContext =
    React.useMemo<InteractionVisualizationContext>(
      () => ({
        activeNodeIds: interactionActiveNodeIds,
        visitedNodeIds: interactionVisitedNodeIds,
        isWorkflowRunning,
        setWorkflowRunning,
        clearInteractionVisualization,
        markInteractionFinished,
        markInteractionOperationFinished,
        markInteractionOperationStarted,
      }),
      [
        clearInteractionVisualization,
        interactionActiveNodeIds,
        interactionVisitedNodeIds,
        isWorkflowRunning,
        markInteractionFinished,
        markInteractionOperationFinished,
        markInteractionOperationStarted,
      ],
    );
  const savedNodes = React.useRef<DiagramEditorNode[]>([]);

  const [edges, setEdges] = React.useState<DiagramEditorEdge[]>([]);
  const [reconnectingEdgeId, setReconnectingEdgeId] = React.useState<string>();
  const [hoveredEdgeId, setHoveredEdgeId] = React.useState<string>();
  const [selectedEdgeId, setSelectedEdgeId] = React.useState<string>();
  const clearEdgeInspection = () => {
    setHoveredEdgeId(undefined);
    setSelectedEdgeId(undefined);
  };
  const reconnectingEdge = React.useRef<string | undefined>(undefined);
  const savedEdges = React.useRef<DiagramEditorEdge[]>([]);

  const [templates, setTemplates] = useTemplates();
  const registry = useRegistry();
  const [diagramProperties, setDiagramProperties] = useDiagramProperties();
  const compatibility = useCompatibilityGraph(
    {
      registry,
      nodeManager,
      edges,
      templates,
      diagramProperties,
      templateId:
        editorMode.mode === EditorMode.Template
          ? editorMode.templateId
          : undefined,
    },
    reconnectingEdgeId,
  );
  const displayEdges = React.useMemo(
    () =>
      edges.map((edge) =>
        !isWorkflowRunning &&
        compatibility.edgeResults.get(edge.id)?.status === 'incompatible'
          ? {
              ...edge,
              className: [edge.className, 'edge-incompatible']
                .filter(Boolean)
                .join(' '),
            }
          : edge,
      ),
    [edges, compatibility.edgeResults, isWorkflowRunning],
  );
  const inspectedEdgeId = hoveredEdgeId ?? selectedEdgeId;
  const {
    drafts: transientEditorDrafts,
    replaceDrafts: replaceTransientEditorDrafts,
    clearDrafts: clearTransientEditorDrafts,
    clearOperationConfigDrafts,
    hasUncommittedBuffers,
  } = useTransientEditorDrafts();
  const openScriptEnvironment = useScriptEnvironmentNavigation();
  const {
    state: {
      open: sidePanelOpen,
      expanded: sidePanelExpanded,
      tab: sidePanelTab,
    },
  } = useDiagramSidePanel();

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

      clearOperationConfigDrafts(removedNodes);

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
    [clearOperationConfigDrafts, handleEdgeChanges, nodeManager, edges],
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
    sourceConnection: {
      sourceNodeId: string;
      sourceHandle: string | null;
      sourceHandleType: 'source' | 'target';
    } | null;
  }>({
    open: false,
    popOverPosition: { left: 0, top: 0 },
    parentId: null,
    sourceConnection: null,
  });
  const addOperationPopoverActions = React.useRef<PopoverActions>(null);
  const updateAddOperationPopoverPosition = React.useCallback(() => {
    addOperationPopoverActions.current?.updatePosition();
  }, []);
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
  const [pendingScriptEnvironmentNodeId, setPendingScriptEnvironmentNodeId] =
    React.useState<string | null>(null);

  React.useEffect(() => {
    if (!sidePanelOpen || sidePanelTab !== 'environments') {
      setPendingScriptEnvironmentNodeId(null);
    }
  }, [sidePanelOpen, sidePanelTab]);

  const scriptNodeBinding = React.useMemo<
    ScriptNodeEnvironmentBinding | undefined
  >(() => {
    const nodeId = editingNodeId || pendingScriptEnvironmentNodeId;
    const node = nodeId ? nodeManager.tryGetNode(nodeId) : undefined;
    if (!node || node.type !== 'script') {
      return undefined;
    }

    const environmentName = node.data.op.environment;
    return {
      environmentName:
        typeof environmentName === 'string'
          ? environmentName || undefined
          : undefined,
      nodeControlsSelection: editingNodeId === node.id,
      assignEnvironment: (environmentName) => {
        handleNodeChange({
          type: 'replace',
          id: node.id,
          item: {
            ...node,
            data: {
              ...node.data,
              op: { ...node.data.op, environment: environmentName },
            },
          },
        });
        if (pendingScriptEnvironmentNodeId === node.id) {
          setPendingScriptEnvironmentNodeId(null);
        }
      },
    };
  }, [
    editingNodeId,
    handleNodeChange,
    nodeManager,
    pendingScriptEnvironmentNodeId,
  ]);

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
    setPendingScriptEnvironmentNodeId(null);
    setEditingEdgeId(null);
    setAddOperationPopover((prev) => ({
      ...prev,
      open: false,
      sourceConnection: null,
    }));
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
                sourceConnection: null,
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
    setEnableExport(false);
  }, []);
  const [loadContext, setLoadContext] = React.useState<LoadContext | null>(
    null,
  );
  const [recentlyUsedFilename, setRecentlyUsedFilename] = React.useState<
    string | null
  >(null);
  const [hydrationResolved, setHydrationResolved] = React.useState(false);
  const [isDirty, setIsDirty] = React.useState(false);
  const dirtyRef = React.useRef(false);
  const baselineFingerprint = React.useRef<string | null>(null);
  const markCleanOnNextSnapshot = React.useRef(false);
  const latestDraftContent = React.useRef<DraftWorkspaceContent | null>(null);
  const autosaveTimer = React.useRef<ReturnType<typeof setTimeout> | null>(
    null,
  );
  const [draftStorageFailed, setDraftStorageFailed] = React.useState(false);
  const [recoveryCandidate, setRecoveryCandidate] =
    React.useState<RecoveryCandidate | null>(null);

  const clearStoredDraft = React.useCallback(() => {
    try {
      clearDraftWorkspace();
      setDraftStorageFailed(false);
    } catch {
      setDraftStorageFailed(true);
    }
  }, []);

  const prepareDiagram = React.useCallback(
    async (
      jsonStr: string,
      filename: string | null,
    ): Promise<PreparedDiagram> => {
      const [diagram, loaded] = await loadDiagramJson(jsonStr);
      return { diagram, loaded, filename };
    },
    [],
  );

  const applyPreparedDiagram = React.useCallback(
    (prepared: PreparedDiagram) => {
      const { diagram, loaded, filename } = prepared;
      const { graph, isRestored } = loaded;
      clearInteractionVisualization();
      setLoadContext({ diagram });
      setDiagramProperties({
        description: diagram.description ?? '',
        input_examples: diagram.input_examples ?? [],
        script_environments: diagram.script_environments ?? {},
      });
      const nextNodes = isRestored
        ? graph.nodes
        : applyNodeChanges(
            autoLayout(graph.nodes, graph.edges, LAYOUT_OPTIONS),
            graph.nodes,
          );
      savedNodes.current = [];
      savedEdges.current = [];
      setEditorMode({ mode: EditorMode.Normal });
      setNodes(nextNodes);
      setEdges(graph.edges);
      setTemplates(diagram.templates || {});
      setRecentlyUsedFilename(filename);
      clearTransientEditorDrafts();
      closeAllPopovers();
      markCleanOnNextSnapshot.current = true;
      setHydrationResolved(true);
      clearStoredDraft();
      requestAnimationFrame(() => reactFlowInstance.current?.fitView());
    },
    [
      clearInteractionVisualization,
      clearStoredDraft,
      clearTransientEditorDrafts,
      closeAllPopovers,
      setDiagramProperties,
    ],
  );

  const loadDiagram = React.useCallback(
    async (jsonStr: string, filename: string | null) => {
      try {
        const prepared = await prepareDiagram(jsonStr, filename);
        if (
          (isDirty || hasUncommittedBuffers) &&
          !window.confirm(
            'Replace the current diagram and discard its unsaved changes?',
          )
        ) {
          return;
        }
        applyPreparedDiagram(prepared);
      } catch (e) {
        showErrorToast(`failed to load diagram: ${e}`);
      }
    },
    [
      applyPreparedDiagram,
      hasUncommittedBuffers,
      isDirty,
      prepareDiagram,
      showErrorToast,
    ],
  );

  const [openExportDiagramDialog, setOpenExportDiagramDialog] =
    React.useState(false);
  const [openNewDiagramDialog, setOpenNewDiagramDialog] = React.useState(false);

  const handleMouseDown = React.useCallback(() => {
    mouseDownTime.current = Date.now();
  }, []);

  const getClientPosition = React.useCallback(
    (event: MouseEvent | TouchEvent): XYPosition | null => {
      if ('clientX' in event) {
        return { x: event.clientX, y: event.clientY };
      }

      const touch = event.changedTouches[0] || event.touches[0];
      if (!touch) {
        return null;
      }

      return { x: touch.clientX, y: touch.clientY };
    },
    [],
  );

  const tryCreateEdge = (
    connection: Connection,
    edgeId?: string,
    nodeChanges: Extract<NodeChange<DiagramEditorNode>, { type: 'add' }>[] = [],
  ): DiagramEditorEdge | null => {
    const result = prepareConnection({
      connection,
      edgeId,
      nodeManager: nodeChanges.length
        ? new NodeManager([...nodes, ...nodeChanges.map(({ item }) => item)])
        : nodeManager,
      edges,
      registry,
      templates,
    });
    if (!result.valid) {
      showErrorToast(result.error);
      return null;
    }
    return result.edge;
  };

  const [enableExport, setEnableExport] = React.useState(true);

  const draftContent = React.useMemo<DraftWorkspaceContent>(() => {
    const {
      highlightedEnvironment: _highlightedEnvironment,
      ...persistedDiagramProperties
    } = diagramProperties;
    const mainGraph =
      editorMode.mode === EditorMode.Template
        ? snapshotGraph(savedNodes.current, savedEdges.current)
        : snapshotGraph(nodes, edges);
    const activeTemplate =
      editorMode.mode === EditorMode.Template
        ? {
            templateId: editorMode.templateId,
            graph: snapshotGraph(nodes, edges),
          }
        : undefined;
    return {
      mainGraph,
      activeTemplate,
      templates: cloneSerializable(templates),
      diagramProperties: cloneSerializable(persistedDiagramProperties),
      sourceExtensions: loadContext?.diagram.extensions
        ? cloneSerializable(loadContext.diagram.extensions)
        : undefined,
      filename: recentlyUsedFilename,
      transientEditors: cloneSerializable(transientEditorDrafts),
    };
  }, [
    diagramProperties,
    edges,
    editorMode,
    loadContext,
    nodes,
    recentlyUsedFilename,
    templates,
    transientEditorDrafts,
  ]);

  const persistLatestDraft = React.useCallback(() => {
    if (!dirtyRef.current || !latestDraftContent.current) {
      return;
    }
    try {
      writeDraftWorkspace(latestDraftContent.current);
      setDraftStorageFailed(false);
    } catch {
      setDraftStorageFailed(true);
    }
  }, []);

  React.useEffect(() => {
    latestDraftContent.current = draftContent;
    if (!hydrationResolved) {
      return;
    }

    const fingerprint = draftWorkspaceFingerprint(draftContent);
    if (markCleanOnNextSnapshot.current) {
      markCleanOnNextSnapshot.current = false;
      baselineFingerprint.current = fingerprint;
      dirtyRef.current = false;
      setIsDirty(false);
      clearStoredDraft();
      return;
    }
    if (baselineFingerprint.current === null) {
      baselineFingerprint.current = fingerprint;
      dirtyRef.current = false;
      setIsDirty(false);
      return;
    }

    const dirty = fingerprint !== baselineFingerprint.current;
    dirtyRef.current = dirty;
    setIsDirty(dirty);
    if (!dirty) {
      clearStoredDraft();
      return;
    }

    if (autosaveTimer.current !== null) {
      clearTimeout(autosaveTimer.current);
    }
    autosaveTimer.current = setTimeout(persistLatestDraft, 500);
    return () => {
      if (autosaveTimer.current !== null) {
        clearTimeout(autosaveTimer.current);
        autosaveTimer.current = null;
      }
    };
  }, [clearStoredDraft, draftContent, hydrationResolved, persistLatestDraft]);

  useDraftPagehideFlush(
    latestDraftContent,
    hydrationResolved && (isDirty || hasUncommittedBuffers),
    setDraftStorageFailed,
  );

  useBeforeUnloadWarning(isDirty || hasUncommittedBuffers);

  const resetToNewDiagram = React.useCallback(() => {
    const empty = loadEmpty();
    clearInteractionVisualization();
    closeAllPopovers();
    savedNodes.current = [];
    savedEdges.current = [];
    setEditorMode({ mode: EditorMode.Normal });
    setNodes(empty.nodes);
    setEdges(empty.edges);
    setTemplates({});
    setDiagramProperties(createEmptyDiagramProperties());
    setLoadContext(null);
    setRecentlyUsedFilename(null);
    clearTransientEditorDrafts();
    markCleanOnNextSnapshot.current = true;
    setHydrationResolved(true);
    clearStoredDraft();
    requestAnimationFrame(() => reactFlowInstance.current?.fitView());
  }, [
    clearInteractionVisualization,
    clearStoredDraft,
    clearTransientEditorDrafts,
    closeAllPopovers,
    setDiagramProperties,
  ]);

  const handleNewDiagram = React.useCallback(() => {
    if (isDirty || hasUncommittedBuffers) {
      setOpenNewDiagramDialog(true);
      return;
    }
    resetToNewDiagram();
  }, [hasUncommittedBuffers, isDirty, resetToNewDiagram]);

  const restoreRecoveryDraft = React.useCallback(() => {
    if (!recoveryCandidate) {
      return;
    }
    const { draft } = recoveryCandidate;
    savedNodes.current = cloneSerializable(draft.mainGraph.nodes);
    savedEdges.current = cloneSerializable(draft.mainGraph.edges);
    if (draft.activeTemplate) {
      setNodes(cloneSerializable(draft.activeTemplate.graph.nodes));
      setEdges(cloneSerializable(draft.activeTemplate.graph.edges));
      setEditorMode({
        mode: EditorMode.Template,
        templateId: draft.activeTemplate.templateId,
      });
    } else {
      setNodes(cloneSerializable(draft.mainGraph.nodes));
      setEdges(cloneSerializable(draft.mainGraph.edges));
      setEditorMode({ mode: EditorMode.Normal });
    }
    setTemplates(cloneSerializable(draft.templates));
    setDiagramProperties(cloneSerializable(draft.diagramProperties));
    setRecentlyUsedFilename(draft.filename);
    replaceTransientEditorDrafts(cloneSerializable(draft.transientEditors));
    const restoredDiagram: Diagram = {
      version: '0.1.0',
      start: { builtin: 'dispose' },
      ops: {},
      templates: cloneSerializable(draft.templates),
      description: draft.diagramProperties.description,
      input_examples: draft.diagramProperties.input_examples,
      script_environments: draft.diagramProperties.script_environments,
      extensions: draft.sourceExtensions,
    };
    setLoadContext({ diagram: restoredDiagram });
    baselineFingerprint.current = '__restored_recovery_draft__';
    dirtyRef.current = true;
    setIsDirty(true);
    setHydrationResolved(true);
    setRecoveryCandidate(null);
    requestAnimationFrame(() => reactFlowInstance.current?.fitView());
  }, [recoveryCandidate, replaceTransientEditorDrafts, setDiagramProperties]);

  const discardRecoveryDraft = React.useCallback(() => {
    if (!recoveryCandidate) {
      return;
    }
    const linkedDiagram = recoveryCandidate.linkedDiagram;
    setRecoveryCandidate(null);
    clearStoredDraft();
    if (linkedDiagram) {
      applyPreparedDiagram(linkedDiagram);
      return;
    }
    const empty = loadEmpty();
    setNodes(empty.nodes);
    setEdges(empty.edges);
    setTemplates({});
    setDiagramProperties(createEmptyDiagramProperties());
    setLoadContext(null);
    setRecentlyUsedFilename(null);
    clearTransientEditorDrafts();
    baselineFingerprint.current = null;
    markCleanOnNextSnapshot.current = true;
    setHydrationResolved(true);
  }, [
    applyPreparedDiagram,
    clearStoredDraft,
    clearTransientEditorDrafts,
    recoveryCandidate,
    setDiagramProperties,
  ]);

  const markExportCompleted = React.useCallback(
    (filename: string) => {
      setRecentlyUsedFilename(filename);
      if (latestDraftContent.current) {
        const exportedContent = {
          ...latestDraftContent.current,
          filename,
        };
        latestDraftContent.current = exportedContent;
        baselineFingerprint.current =
          draftWorkspaceFingerprint(exportedContent);
      }
      markCleanOnNextSnapshot.current = false;
      dirtyRef.current = false;
      setIsDirty(false);
      clearStoredDraft();
    },
    [clearStoredDraft],
  );

  const handleStartNewAfterExport = React.useCallback(() => {
    setOpenExportDiagramDialog(false);
    resetToNewDiagram();
  }, [resetToNewDiagram]);

  const newDiagramAfterExport = useNewDiagramAfterExport(
    markExportCompleted,
    handleStartNewAfterExport,
  );

  const startupInitialized = React.useRef(false);

  return (
    <Providers
      editorModeContext={[editorMode, updateEditorModeAction]}
      interactionVisualizationContext={interactionVisualizationContext}
      loadContext={loadContext}
      nodeManager={nodeManager}
      edges={edges}
      compatibility={compatibility}
    >
      <ReactFlow
        nodes={nodes}
        edges={displayEdges}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        nodeTypes={NODE_TYPES}
        edgeTypes={EDGE_TYPES}
        onInit={(instance) => {
          reactFlowInstance.current = instance;
          if (startupInitialized.current) {
            return;
          }
          startupInitialized.current = true;

          void (async () => {
            let linkedDiagram: PreparedDiagram | null = null;
            const diagramParam = new URLSearchParams(
              window.location.search,
            ).get('diagram');
            if (diagramParam) {
              try {
                linkedDiagram = await prepareDiagram(
                  decodeDiagramParam(diagramParam),
                  null,
                );
              } catch (error) {
                showErrorToast(
                  `failed to load linked diagram: ${
                    error instanceof Error ? error.message : error
                  }`,
                );
              }
            }

            let storedDraft: DraftWorkspaceV1 | null = null;
            try {
              storedDraft = readDraftWorkspace();
            } catch (error) {
              clearStoredDraft();
              showErrorToast(
                `failed to read recovery draft: ${
                  error instanceof Error ? error.message : error
                }`,
              );
            }

            if (storedDraft) {
              setRecoveryCandidate({ draft: storedDraft, linkedDiagram });
              return;
            }
            if (linkedDiagram) {
              applyPreparedDiagram(linkedDiagram);
              return;
            }
            markCleanOnNextSnapshot.current = true;
            setHydrationResolved(true);
          })();
        }}
        onNodesChange={handleNodeChanges}
        onNodesDelete={() => {
          closeAllPopovers();
        }}
        onEdgesChange={handleEdgeChanges}
        onEdgesDelete={() => {
          clearEdgeInspection();
          closeAllPopovers();
        }}
        onConnectStart={clearEdgeInspection}
        onConnect={(conn) => {
          clearEdgeInspection();
          const newEdge = tryCreateEdge(conn);
          if (newEdge) {
            setEdges((prev) => addEdge(newEdge, prev));
          }
        }}
        isValidConnection={(conn) => {
          return validateConnectionSimple(
            conn,
            nodeManager,
            edges.filter((edge) => edge.id !== reconnectingEdge.current),
          ).valid;
        }}
        onReconnectStart={(_, edge) => {
          clearEdgeInspection();
          reconnectingEdge.current = edge.id;
          setReconnectingEdgeId(edge.id);
        }}
        onReconnectEnd={() => {
          reconnectingEdge.current = undefined;
          setReconnectingEdgeId(undefined);
        }}
        onReconnect={(oldEdge, newConnection) => {
          const newEdge = tryCreateEdge(newConnection, oldEdge.id);
          if (newEdge) {
            setEdges((prev) => {
              // React Flow's oldEdge includes display-only diagnostics.
              const updatedEdge = {
                ...prev.find((edge) => edge.id === oldEdge.id),
                type: newEdge.type,
                data: newEdge.data,
              } as DiagramEditorEdge;
              return reconnectEdge(updatedEdge, newConnection, prev);
            });
          }
        }}
        onConnectEnd={(event, connectionState) => {
          clearEdgeInspection();
          // React Flow also calls this new-wire handler when reconnecting.
          if (reconnectingEdge.current && !connectionState.toHandle) {
            return;
          }
          if (!connectionState.fromHandle) {
            return;
          }

          if (connectionState.isValid === false && connectionState.toHandle) {
            const direction = validateDraggedHandlePair({
              fromHandleType: connectionState.fromHandle.type,
              otherHandleType: connectionState.toHandle.type,
            });
            if (!direction.valid) {
              showErrorToast(direction.error);
              return;
            }

            const result = validateConnectionSimple(
              createConnectionFromHandles(
                connectionState.fromHandle,
                connectionState.toHandle.nodeId,
                connectionState.toHandle.id,
              ),
              nodeManager,
              edges.filter((edge) => edge.id !== reconnectingEdge.current),
            );

            if (!result.valid) {
              showErrorToast(result.error);
            }
            return;
          }

          if (connectionState.toHandle) {
            return;
          }

          const sourceNode = nodeManager.tryGetNode(
            connectionState.fromHandle.nodeId,
          );
          const clientPosition = getClientPosition(event);
          if (!sourceNode || !clientPosition) {
            return;
          }

          if (connectionState.fromHandle.type === 'source') {
            const outputCapacity = validateSourceOutputCapacity(
              sourceNode,
              connectionState.fromHandle.id,
              edges,
            );
            if (!outputCapacity.valid) {
              showErrorToast(outputCapacity.error);
              return;
            }
          }

          setAddOperationPopover({
            open: true,
            popOverPosition: { left: clientPosition.x, top: clientPosition.y },
            parentId: sourceNode.parentId || null,
            sourceConnection: {
              sourceNodeId: sourceNode.id,
              sourceHandle: connectionState.fromHandle.id || null,
              sourceHandleType: connectionState.fromHandle.type,
            },
          });
          suppressNextPaneClick.current = true;
        }}
        onNodeClick={(ev, node) => {
          ev.stopPropagation();
          closeAllPopovers();

          if (isBuiltinNode(node)) {
            return;
          }
          if (node.type === 'script') {
            const environmentName = node.data.op.environment;
            openScriptEnvironment(
              typeof environmentName === 'string'
                ? environmentName || undefined
                : undefined,
              false,
            );
          }
          setEditingNodeId(node.id);
          setEditOpFormPopoverProps({
            open: true,
            anchorReference: 'anchorPosition',
            anchorPosition: getEditPopoverPositionForNode({
              nodeRect: ev.currentTarget.getBoundingClientRect(),
              viewportWidth: window.innerWidth,
              sidePanel: {
                open: sidePanelOpen,
                expanded: sidePanelExpanded,
              },
            }),
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
          setSelectedEdgeId(edge.id);

          setEditOpFormPopoverProps({
            open: true,
            anchorReference: 'anchorPosition',
            anchorPosition: { left: ev.clientX, top: ev.clientY },
          });
        }}
        onEdgeMouseEnter={(_, edge) => setHoveredEdgeId(edge.id)}
        onEdgeMouseLeave={() => setHoveredEdgeId(undefined)}
        onSelectionChange={({ edges: selected }) =>
          setSelectedEdgeId(selected[0]?.id)
        }
        onPaneClick={(ev) => {
          clearEdgeInspection();
          if (suppressNextPaneClick.current) {
            suppressNextPaneClick.current = false;
            return;
          }

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
            sourceConnection: null,
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
        <ConnectionHintPanel
          nodeManager={nodeManager}
          reconnectingEdgeId={reconnectingEdgeId}
          edgeResult={
            inspectedEdgeId
              ? compatibility.edgeResults.get(inspectedEdgeId)
              : undefined
          }
        />
        <CommandPanel
          onNodeChanges={handleNodeChanges}
          onNewDiagram={handleNewDiagram}
          onExportClick={React.useCallback(
            () => setOpenExportDiagramDialog(true),
            [],
          )}
          onLoadDiagram={loadDiagram}
          enableExport={enableExport && !hasUncommittedBuffers}
          exportDisabledReason={
            hasUncommittedBuffers
              ? 'Save or discard unfinished editor fields before exporting'
              : undefined
          }
          isDirty={isDirty}
          scriptNodeBinding={scriptNodeBinding}
        />
        {editorMode.mode === EditorMode.Template && (
          <Fab
            color="primary"
            aria-label="Save"
            sx={{ position: 'absolute', right: 64, bottom: 64 }}
            onClick={() => {
              const exportedTemplate = exportTemplate(
                registry,
                nodeManager,
                edges,
              );
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
          action={addOperationPopoverActions}
          open={addOperationPopover.open}
          onClose={closeAllPopovers}
          anchorReference="anchorPosition"
          anchorPosition={addOperationPopover.popOverPosition}
          // use a custom component to prevent the popover from creating an invisible element that blocks clicks
          component={NonCapturingPopoverContainer}
        >
          {addOperationPopover.sourceConnection ? (
            <CompatibleAddOperation
              parentId={addOperationPopover.parentId || undefined}
              newNodePosition={addOperationNewNodePosition}
              sourceConnection={addOperationPopover.sourceConnection}
              onContentChange={updateAddOperationPopoverPosition}
              onAdd={({ changes, primaryNodeId }) => {
                const targetNode =
                  changes.find((change) => change.item.id === primaryNodeId)
                    ?.item || null;
                if (!targetNode || !addOperationPopover.sourceConnection) {
                  return;
                }

                const connection = createConnectionFromHandles(
                  {
                    nodeId: addOperationPopover.sourceConnection.sourceNodeId,
                    id: addOperationPopover.sourceConnection.sourceHandle,
                    type: addOperationPopover.sourceConnection.sourceHandleType,
                  },
                  targetNode.id,
                  null,
                );
                const newEdge = tryCreateEdge(connection, undefined, changes);
                if (!newEdge) {
                  return;
                }

                handleNodeChanges(changes);
                setEdges((prev) => addEdge(newEdge, prev));
                closeAllPopovers();
                if (targetNode.type === 'script') {
                  const environmentName = targetNode.data.op.environment;
                  openScriptEnvironment(
                    typeof environmentName === 'string'
                      ? environmentName || undefined
                      : undefined,
                    true,
                  );
                  setPendingScriptEnvironmentNodeId(targetNode.id);
                }
              }}
            />
          ) : (
            <AddOperation
              parentId={addOperationPopover.parentId || undefined}
              newNodePosition={addOperationNewNodePosition}
              onAdd={({ changes, primaryNodeId }) => {
                handleNodeChanges(changes);
                const primaryNode =
                  changes.find((change) => change.item.id === primaryNodeId)
                    ?.item || null;
                closeAllPopovers();
                if (primaryNode?.type === 'script') {
                  const environmentName = primaryNode.data.op.environment;
                  openScriptEnvironment(
                    typeof environmentName === 'string'
                      ? environmentName || undefined
                      : undefined,
                    true,
                  );
                  setPendingScriptEnvironmentNodeId(primaryNode.id);
                }
              }}
            />
          )}
        </Popover>
        <ResponsiveEditPopover
          {...editOpFormPopoverProps}
          onClose={(_event, reason) => {
            if (
              reason === 'escapeKeyDown' &&
              shouldIgnoreEscapeClose(document.activeElement)
            ) {
              return;
            }
            setEditOpFormPopoverProps({ open: false });
          }}
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
              nodeManager={nodeManager}
              onChange={handleEdgeChange}
              onDelete={(changes) => {
                setEdges((prev) => applyEdgeChanges([changes], prev));
                closeAllPopovers();
              }}
            />
          )}
        </ResponsiveEditPopover>
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
        <Snackbar
          open={draftStorageFailed}
          anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
        >
          <Alert severity="warning">
            Draft recovery is unavailable. Save your diagram to a file before
            leaving.
          </Alert>
        </Snackbar>
        <NewDiagramDialog
          open={openNewDiagramDialog}
          canSave={enableExport && !hasUncommittedBuffers}
          onCancel={() => setOpenNewDiagramDialog(false)}
          onDiscard={() => {
            setOpenNewDiagramDialog(false);
            resetToNewDiagram();
          }}
          onSave={() => {
            if (!enableExport || hasUncommittedBuffers) {
              return;
            }
            setOpenNewDiagramDialog(false);
            newDiagramAfterExport.begin();
            setOpenExportDiagramDialog(true);
          }}
        />
        <Dialog open={recoveryCandidate !== null} disableEscapeKeyDown>
          <DialogTitle>Restore previous diagram?</DialogTitle>
          <DialogContent>
            {recoveryCandidate?.linkedDiagram
              ? 'Restore the previous diagram from this tab, or replace it with the linked diagram.'
              : 'Restore the diagram from before the refresh, or start with an empty diagram.'}
          </DialogContent>
          <DialogActions>
            <Button onClick={discardRecoveryDraft}>
              {recoveryCandidate?.linkedDiagram
                ? 'Load Linked Diagram'
                : 'Start Empty'}
            </Button>
            <Button variant="contained" onClick={restoreRecoveryDraft}>
              Restore
            </Button>
          </DialogActions>
        </Dialog>
        <Suspense>
          <ExportDiagramDialog
            open={openExportDiagramDialog}
            suggestedFilename={recentlyUsedFilename}
            onExportCompleted={newDiagramAfterExport.complete}
            onClose={() => {
              setOpenExportDiagramDialog(false);
              newDiagramAfterExport.cancel();
            }}
            onValidDiagram={(maybeValid: MaybeValid) => {
              setEnableExport(maybeValid.ok);
              if (!maybeValid.ok) {
                showErrorToast(maybeValid.errorMessage);
              }
            }}
          />
        </Suspense>
      </ReactFlow>
    </Providers>
  );
}

export default DiagramEditor;
