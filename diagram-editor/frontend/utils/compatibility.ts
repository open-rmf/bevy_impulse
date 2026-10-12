import type { Connection, NodeAddChange } from '@xyflow/react';
import { firstValueFrom } from 'rxjs';
import type { BaseApiClient } from '../api-client';
import type { DiagramProperties } from '../diagram-properties-provider';
import type { DiagramEditorEdge } from '../edges';
import { NodeManager } from '../node-manager';
import {
  type DiagramEditorNode,
  isBuiltinNode,
  isOperationNode,
} from '../nodes';
import type {
  CompatibilityConnection,
  CompatibilityRequest,
  CompatibilityResult,
  Diagram,
  DiagramElementMetadata,
  NamespaceList,
  OperationRef,
  OutputKey,
  OutputRef,
  PortRef,
  SectionTemplate,
} from '../types/api';
import {
  createEdgeFromConnection,
  type EdgeCreationResult,
  validateEdgeSimple,
} from './connection';
import { exportDiagram, exportTemplate } from './export-diagram';
import { ROOT_NAMESPACE, splitNamespaces } from './namespace';

const TEMPLATE_SECTION = '__template__';

function cloneJson<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function namespaceList(
  namespace: string,
  templateSection?: string,
): NamespaceList {
  const namespaces = splitNamespaces(namespace).filter(
    (part) => part !== ROOT_NAMESPACE,
  );
  return templateSection ? [templateSection, ...namespaces] : namespaces;
}

function namedOperation(
  namespaces: NamespaceList,
  name: string,
  exposedNamespace?: string | null,
): OperationRef {
  return {
    named: {
      namespaces,
      exposed_namespace: exposedNamespace ?? null,
      name,
    },
  };
}

function namedOutput(
  namespaces: NamespaceList,
  operation: string,
  key: OutputKey,
): OutputRef {
  return {
    Named: {
      namespaces,
      operation,
      key,
    },
  };
}

function inputPort(operation: OperationRef): PortRef {
  return { Input: operation };
}

function outputPort(output: OutputRef): PortRef {
  return { Output: output };
}

function operationInputPort(
  node: DiagramEditorNode,
  edge?: DiagramEditorEdge,
  templateSection?: string,
): PortRef | null {
  if (isBuiltinNode(node)) {
    switch (node.type) {
      case 'terminate': {
        return inputPort({
          terminate: namespaceList(node.data.namespace, templateSection),
        });
      }
      case 'start': {
        return null;
      }
    }
  }

  if (isOperationNode(node)) {
    if (
      node.type === 'section' &&
      (edge?.data.input.type === 'sectionInput' ||
        edge?.data.input.type === 'sectionBuffer')
    ) {
      return inputPort(
        namedOperation(
          namespaceList(node.data.namespace, templateSection),
          edge.data.input.inputId,
          node.data.opId,
        ),
      );
    }

    return inputPort(
      namedOperation(
        namespaceList(node.data.namespace, templateSection),
        node.data.opId,
      ),
    );
  }

  if (node.type === 'sectionOutput') {
    if (templateSection) {
      return outputPort(
        namedOutput([], templateSection, ['connect', node.data.outputId]),
      );
    }
    return inputPort(namedOperation([], node.data.outputId));
  }

  return null;
}

function forkCloneOutputIndex(
  edge: DiagramEditorEdge,
  edges: DiagramEditorEdge[],
): number {
  return edges
    .filter((candidate) => candidate.source === edge.source)
    .findIndex((candidate) => candidate.id === edge.id);
}

function operationOutputKey(
  sourceNode: DiagramEditorNode,
  edge: DiagramEditorEdge,
  edges: DiagramEditorEdge[],
): OutputKey | null {
  switch (edge.type) {
    case 'default': {
      if (sourceNode.type === 'fork_clone') {
        return ['next', Math.max(0, forkCloneOutputIndex(edge, edges))];
      }
      return ['next'];
    }
    case 'forkResultOk': {
      return ['ok'];
    }
    case 'forkResultErr': {
      return ['err'];
    }
    case 'splitKey': {
      return ['keyed', edge.data.output.key];
    }
    case 'splitSeq':
    case 'unzip': {
      return ['next', edge.data.output.seq];
    }
    case 'splitRemaining': {
      return ['remaining'];
    }
    case 'streamOut': {
      return ['stream_out', edge.data.output.streamId];
    }
    case 'section': {
      return ['connect', edge.data.output.output];
    }
    case 'buffer': {
      return null;
    }
  }
}

function operationOutputPort(
  node: DiagramEditorNode,
  edge: DiagramEditorEdge,
  edges: DiagramEditorEdge[],
  templateSection?: string,
): PortRef | null {
  if (isBuiltinNode(node)) {
    switch (node.type) {
      case 'start': {
        return outputPort({
          Start: namespaceList(node.data.namespace, templateSection),
        });
      }
      case 'terminate': {
        return null;
      }
    }
  }

  if (isOperationNode(node)) {
    if (edge.type === 'buffer') {
      return inputPort(
        namedOperation(
          namespaceList(node.data.namespace, templateSection),
          node.type === 'section'
            ? (edge.data.output.bufferId ?? '')
            : node.data.opId,
          node.type === 'section' ? node.data.opId : undefined,
        ),
      );
    }

    const key = operationOutputKey(node, edge, edges);
    if (!key) {
      return null;
    }

    return outputPort(
      namedOutput(
        namespaceList(node.data.namespace, templateSection),
        node.data.opId,
        key,
      ),
    );
  }

  if (node.type === 'sectionInput' || node.type === 'sectionBuffer') {
    return inputPort(namedOperation([], node.data.remappedId, templateSection));
  }

  return null;
}

function portRefsForEdge(
  nodeManager: NodeManager,
  edge: DiagramEditorEdge,
  edges: DiagramEditorEdge[],
  templateSection?: string,
): Pick<CompatibilityConnection, 'focusPorts' | 'sourcePort' | 'targetPort'> {
  const sourceNode = nodeManager.getNode(edge.source);
  const targetNode = nodeManager.getNode(edge.target);

  if (edge.type === 'buffer') {
    const bufferPort = operationOutputPort(
      sourceNode,
      edge,
      edges,
      templateSection,
    );
    const focusPorts = bufferPort ? [bufferPort] : [];
    if (isOperationNode(targetNode)) {
      focusPorts.push(
        outputPort(
          namedOutput(
            namespaceList(targetNode.data.namespace, templateSection),
            targetNode.data.opId,
            ['next'],
          ),
        ),
      );
    }

    return { focusPorts };
  }

  const sourcePort =
    operationOutputPort(sourceNode, edge, edges, templateSection) ?? undefined;
  const targetPort =
    operationInputPort(targetNode, edge, templateSection) ?? undefined;
  const focusPorts = [sourcePort, targetPort].filter((port): port is PortRef =>
    Boolean(port),
  );

  // Interface aliases describe the edited template, but carry no external
  // producer or consumer type until a concrete section instance is connected.
  return {
    focusPorts,
    sourcePort:
      templateSection &&
      (sourceNode.type === 'sectionInput' ||
        sourceNode.type === 'sectionBuffer')
        ? undefined
        : sourcePort,
    targetPort:
      templateSection && targetNode.type === 'sectionOutput'
        ? undefined
        : targetPort,
  };
}

export interface CompatibilityGraph {
  registry: DiagramElementMetadata;
  nodeManager: NodeManager;
  edges: DiagramEditorEdge[];
  templates: Record<string, SectionTemplate>;
  diagramProperties: DiagramProperties;
  templateId?: string;
}

export function buildCompatibilityRequest(
  graph: CompatibilityGraph,
  focusEdges = graph.edges,
) {
  const nodeManager = graph.nodeManager;
  const focusIds = new Set(focusEdges.map(({ id }) => id));
  const localResults: CompatibilityResult[] = [];
  const edges = cloneJson(graph.edges).filter((edge) => {
    const validation = validateEdgeSimple(edge, nodeManager, graph.edges);
    if (!validation.valid && focusIds.has(edge.id)) {
      localResults.push({
        id: edge.id,
        status: 'incompatible',
        reason: validation.error,
      });
    }
    return validation.valid;
  });
  const validEdgeIds = new Set(edges.map(({ id }) => id));
  const templateSection =
    graph.templateId === undefined ? undefined : TEMPLATE_SECTION;
  const diagram: Diagram =
    graph.templateId === undefined
      ? exportDiagram(
          graph.registry,
          nodeManager,
          edges,
          cloneJson(graph.templates),
          cloneJson(graph.diagramProperties),
        )
      : {
          version: '0.1.0',
          start: { builtin: 'dispose' },
          ops: {
            [TEMPLATE_SECTION]: { type: 'section', template: graph.templateId },
          },
          templates: {
            ...cloneJson(graph.templates),
            [graph.templateId]: exportTemplate(
              graph.registry,
              nodeManager,
              edges,
            ),
          },
          script_environments: cloneJson(graph.diagramProperties)
            .script_environments,
        };
  const request = {
    diagram,
    connections: focusEdges
      .filter(({ id }) => validEdgeIds.has(id))
      .map((edge) => ({
        id: edge.id,
        ...portRefsForEdge(nodeManager, edge, edges, templateSection),
      })),
  } satisfies CompatibilityRequest;
  return { request, localResults };
}

export function prepareConnection({
  connection,
  nodeManager,
  edges,
  registry,
  templates,
  edgeId,
}: {
  connection: Connection;
  nodeManager: NodeManager;
  edges: DiagramEditorEdge[];
  registry: DiagramElementMetadata;
  templates: Record<string, SectionTemplate>;
  edgeId?: string;
}): EdgeCreationResult {
  const edgeResult = createEdgeFromConnection(
    connection,
    nodeManager,
    edges,
    edgeId,
  );
  if (!edgeResult.valid) {
    return edgeResult;
  }
  const { edge } = edgeResult;

  function sectionPorts(
    nodeId: string,
    kind: 'inputs' | 'outputs' | 'buffers',
  ) {
    const node = nodeManager.getNode(nodeId);
    if (node.type !== 'section') return [];
    const op = node.data.op;
    const definition =
      typeof op.builder === 'string'
        ? registry.sections[op.builder]?.interface
        : typeof op.template === 'string'
          ? templates[op.template]
          : undefined;
    const ports = definition?.[kind];
    return Array.isArray(ports) ? ports : Object.keys(ports ?? {});
  }

  if (edge.type === 'section' && !edge.data.output.output) {
    const outputs = sectionPorts(edge.source, 'outputs').filter(
      (output) =>
        !edges.some(
          (existing) =>
            existing.id !== edge.id &&
            existing.source === edge.source &&
            existing.type === 'section' &&
            existing.data.output.output === output,
        ),
    );
    if (outputs.length === 1) edge.data.output = { output: outputs[0] };
  }
  if (edge.type === 'buffer' && !edge.data.output.bufferId) {
    const buffers = sectionPorts(edge.source, 'buffers');
    if (buffers.length === 1) edge.data.output = { bufferId: buffers[0] };
  }
  if (
    (edge.data.input.type === 'sectionInput' ||
      edge.data.input.type === 'sectionBuffer') &&
    !edge.data.input.inputId
  ) {
    const source = nodeManager.getNode(edge.source);
    const inputs = [
      ...(source.type === 'sectionBuffer'
        ? []
        : sectionPorts(edge.target, 'inputs').map((inputId) => ({
            type: 'sectionInput' as const,
            inputId,
          }))),
      ...sectionPorts(edge.target, 'buffers').map((inputId) => ({
        type: 'sectionBuffer' as const,
        inputId,
      })),
    ];
    if (inputs.length === 1) edge.data.input = inputs[0];
  }

  const validation = validateEdgeSimple(edge, nodeManager, edges);
  return validation.valid ? { valid: true, edge } : validation;
}

export function buildConnectionPreview({
  connection,
  nodeChanges = [],
  edgeId,
  ...graph
}: CompatibilityGraph & {
  connection: Connection;
  nodeChanges?: NodeAddChange<DiagramEditorNode>[];
  edgeId?: string;
}) {
  const candidateManager = new NodeManager([
    ...graph.nodeManager.nodes,
    ...nodeChanges.map(({ item }) => item),
  ]);
  const edgeResult = prepareConnection({
    ...graph,
    connection,
    nodeManager: candidateManager,
    edgeId,
  });
  if (!edgeResult.valid)
    return {
      request: null,
      localResults: [
        {
          id: 'preview',
          status: 'incompatible' as const,
          reason: edgeResult.error,
        },
      ],
    };
  const { edge } = edgeResult;

  const built = buildCompatibilityRequest(
    {
      ...graph,
      nodeManager: candidateManager,
      edges: [...graph.edges.filter(({ id }) => id !== edge.id), edge],
    },
    [edge],
  );
  built.request.connections[0].id = 'preview';
  return built;
}

export async function checkCompatibility(
  apiClient: BaseApiClient,
  request: CompatibilityRequest,
): Promise<Map<string, CompatibilityResult>> {
  const response = await firstValueFrom(apiClient.checkCompatibility(request));
  return new Map(response.results.map((result) => [result.id, result]));
}
