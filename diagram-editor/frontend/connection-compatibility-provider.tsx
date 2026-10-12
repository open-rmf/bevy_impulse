import type { Connection, NodeAddChange } from '@xyflow/react';
import { useConnection } from '@xyflow/react';
import React from 'react';
import { useApiClient } from './api-client-provider';
import { NodeManager } from './node-manager';
import type { DiagramEditorNode } from './nodes';
import type { CompatibilityResult } from './types/api';
import {
  buildCompatibilityRequest,
  buildConnectionPreview,
  type CompatibilityGraph,
  checkCompatibility,
} from './utils/compatibility';
import {
  createConnectionFromHandles,
  validateDraggedHandlePair,
} from './utils/connection';

export interface ConnectionPreview {
  connection: Connection;
  nodeChanges?: NodeAddChange<DiagramEditorNode>[];
}

export type ConnectionFeedback = CompatibilityResult & { blocked?: boolean };
const EMPTY_RESULTS: ReadonlyMap<string, CompatibilityResult> = new Map();

function unknownResult(id: string, error: unknown): CompatibilityResult {
  return { id, status: 'unknown', reason: String(error) };
}

export function useCompatibilityGraph(
  graph: CompatibilityGraph,
  reconnectingEdgeId?: string,
) {
  const apiClient = useApiClient();
  // Layout and UI state do not change the workflow's types.
  const serialized = JSON.stringify({
    templateId: graph.templateId,
    registry: graph.registry,
    nodes: graph.nodeManager.nodes.map(({ id, type, parentId, data }) => ({
      id,
      type,
      parentId,
      data,
    })),
    edges: graph.edges.map(
      ({ id, type, source, target, sourceHandle, targetHandle, data }) => ({
        id,
        type,
        source,
        target,
        sourceHandle,
        targetHandle,
        data,
      }),
    ),
    templates: graph.templates,
    diagramProperties: {
      script_environments: graph.diagramProperties.script_environments,
    },
  });
  const snapshot = React.useMemo((): CompatibilityGraph => {
    const { nodes, ...rest } = JSON.parse(serialized);
    return { ...rest, nodeManager: new NodeManager(nodes) };
  }, [serialized]);
  const [diagnostics, setDiagnostics] = React.useState<{
    snapshot: CompatibilityGraph;
    results: ReadonlyMap<string, CompatibilityResult>;
  }>();
  const [menuPreview, setMenuPreview] =
    React.useState<ConnectionPreview | null>(null);

  React.useEffect(() => {
    if (snapshot.edges.length === 0) return;
    let active = true;
    const timer = setTimeout(async () => {
      let results: Map<string, CompatibilityResult>;
      let localResults: CompatibilityResult[] = [];
      try {
        const built = buildCompatibilityRequest(snapshot);
        localResults = built.localResults;
        results = await checkCompatibility(apiClient, built.request);
      } catch (error) {
        results = new Map(
          snapshot.edges.map(({ id }) => [id, unknownResult(id, error)]),
        );
      }
      for (const result of localResults) results.set(result.id, result);
      if (active) setDiagnostics({ snapshot, results });
    }, 150);
    return () => {
      active = false;
      clearTimeout(timer);
    };
  }, [snapshot, apiClient]);

  // A cache belongs to one graph and reconnect operation, not to UI callers.
  const checkConnection = React.useMemo(() => {
    const cache = new Map<string, Promise<ConnectionFeedback>>();
    return (input: ConnectionPreview): Promise<ConnectionFeedback> => {
      const key = JSON.stringify(input);
      const cached = cache.get(key);
      if (cached) return cached;
      const pending = (async (): Promise<ConnectionFeedback> => {
        try {
          const built = buildConnectionPreview({
            ...snapshot,
            ...input,
            edgeId: reconnectingEdgeId,
          });
          if (!built.request)
            return { ...built.localResults[0], blocked: true };
          const results = await checkCompatibility(apiClient, built.request);
          return results.get('preview')!;
        } catch (error) {
          return unknownResult('preview', error);
        }
      })();
      cache.set(key, pending);
      return pending;
    };
  }, [snapshot, reconnectingEdgeId, apiClient]);

  return React.useMemo(
    () => ({
      edgeResults:
        diagnostics?.snapshot === snapshot
          ? diagnostics.results
          : EMPTY_RESULTS,
      checkConnection,
      menuPreview,
      setMenuPreview,
    }),
    [diagnostics, snapshot, checkConnection, menuPreview],
  );
}

const ConnectionCompatibilityContext = React.createContext<ReturnType<
  typeof useCompatibilityGraph
> | null>(null);
export const ConnectionCompatibilityProvider =
  ConnectionCompatibilityContext.Provider;

export function useCompatibilityChecker() {
  const context = React.useContext(ConnectionCompatibilityContext);
  if (!context) throw new Error('Missing ConnectionCompatibilityProvider');
  return context;
}

export function useConnectionCompatibility(
  connection: Connection | null,
  nodeChanges?: NodeAddChange<DiagramEditorNode>[],
): ConnectionFeedback | null {
  const context = React.useContext(ConnectionCompatibilityContext);
  const checkConnection = context?.checkConnection;
  const [response, setResponse] = React.useState<{
    connection: Connection;
    nodeChanges: typeof nodeChanges;
    checker: typeof checkConnection;
    result: ConnectionFeedback;
  }>();
  React.useEffect(() => {
    if (!connection || !checkConnection) return;
    let active = true;
    checkConnection({ connection, nodeChanges }).then((result) => {
      if (active)
        setResponse({
          connection,
          nodeChanges,
          checker: checkConnection,
          result,
        });
    });
    return () => {
      active = false;
    };
  }, [connection, nodeChanges, checkConnection]);
  return response?.connection === connection &&
    response?.nodeChanges === nodeChanges &&
    response?.checker === checkConnection
    ? response.result
    : null;
}

export function useDraggedConnectionCompatibility({
  otherNodeId,
  otherHandleId,
  otherHandleType,
}: {
  otherNodeId: string | null | undefined;
  otherHandleId: string | null | undefined;
  otherHandleType: 'source' | 'target';
}): ConnectionFeedback | null {
  const { inProgress, fromHandle, toHandle } = useConnection();
  const fromNodeId = fromHandle?.nodeId;
  const fromId = fromHandle?.id;
  const fromType = fromHandle?.type;
  const hovered =
    inProgress &&
    toHandle?.nodeId === otherNodeId &&
    (toHandle?.id || null) === (otherHandleId || null) &&
    toHandle?.type === otherHandleType;
  const direction = fromType
    ? validateDraggedHandlePair({
        fromHandleType: fromType,
        otherHandleType,
      })
    : { valid: true as const };
  const connection = React.useMemo(
    () =>
      hovered &&
      fromNodeId &&
      fromType &&
      otherNodeId &&
      fromType !== otherHandleType
        ? createConnectionFromHandles(
            { nodeId: fromNodeId, id: fromId, type: fromType },
            otherNodeId,
            otherHandleId,
          )
        : null,
    [
      hovered,
      fromNodeId,
      fromId,
      fromType,
      otherNodeId,
      otherHandleId,
      otherHandleType,
    ],
  );
  const result = useConnectionCompatibility(connection);
  return hovered && !direction.valid
    ? {
        id: 'preview',
        status: 'incompatible',
        reason: direction.error,
        blocked: true,
      }
    : result;
}
