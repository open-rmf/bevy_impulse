import { of } from 'rxjs';
import type { BaseApiClient } from '../api-client/base-api-client';
import {
  createBufferEdge,
  createDefaultEdge,
  createSectionEdge,
} from '../edges';
import { NodeManager } from '../node-manager';
import { createOperationNode } from '../nodes';
import type { DiagramElementMetadata, SectionTemplate } from '../types/api';
import {
  buildCompatibilityRequest,
  buildConnectionPreview,
  checkCompatibility,
  prepareConnection,
} from './compatibility';
import { loadTemplate } from './load-diagram';
import { ROOT_NAMESPACE } from './namespace';

function operationNode(
  op: Parameters<typeof createOperationNode>[3],
  id: string,
) {
  return createOperationNode(ROOT_NAMESPACE, undefined, { x: 0, y: 0 }, op, id);
}

function connect(source: { id: string }, target: { id: string }) {
  return {
    source: source.id,
    target: target.id,
    sourceHandle: null,
    targetHandle: null,
  };
}

const stubRegistry: DiagramElementMetadata = {
  messages: [],
  nodes: {},
  reverse_message_lookup: {
    result: [],
    split: [],
    unzip: [],
  },
  schemas: {},
  scripting: {},
  sections: {},
  trace_supported: false,
};

describe('isolated template compatibility', () => {
  const editedTemplate: SectionTemplate = {
    inputs: { request: 'processor' },
    outputs: ['response'],
    buffers: { cache: 'storage' },
    ops: {
      processor: { type: 'node', builder: 'updated', next: 'response' },
      storage: { type: 'buffer' },
    },
  };

  function editedGraph() {
    const { nodes, edges } = loadTemplate(editedTemplate);
    return {
      registry: stubRegistry,
      nodeManager: new NodeManager(nodes),
      edges,
      templates: { edited: { ops: {} } },
      templateId: 'edited',
      diagramProperties: {},
    };
  }

  test('exports the currently edited interface and remapping instead of its saved definition', () => {
    const graph = editedGraph();
    const before = JSON.stringify(graph);
    const { request } = buildCompatibilityRequest(graph);
    expect(request.diagram).toMatchObject({
      start: { builtin: 'dispose' },
      ops: { __template__: { type: 'section', template: 'edited' } },
      templates: { edited: editedTemplate },
    });
    expect(request.connections).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          sourcePort: undefined,
          targetPort: expect.anything(),
        }),
        expect.objectContaining({
          sourcePort: expect.anything(),
          targetPort: undefined,
        }),
      ]),
    );
    expect(JSON.stringify(graph)).toBe(before);
  });
});

describe('connection preparation and compatibility previews', () => {
  function bufferGraph() {
    const buffer = operationNode({ type: 'buffer' }, 'buffer');
    const listen = operationNode(
      { type: 'listen', buffers: [], next: { builtin: 'dispose' } },
      'listen',
    );
    const source = operationNode({ type: 'fork_clone', next: [] }, 'source');
    return {
      buffer,
      listen,
      valid: createDefaultEdge(source.id, null, buffer.id, null),
      registry: stubRegistry,
      nodeManager: new NodeManager([buffer, listen, source]),
      templates: {},
      diagramProperties: {},
    };
  }

  test('excludes invalid buffer selections from compatibility export without dropping unrelated edges', () => {
    const graph = bufferGraph();
    const { buffer, listen, valid } = graph;
    const edges = [
      createBufferEdge(buffer.id, null, listen.id, null, {
        type: 'bufferSeq',
        seq: 0,
      }),
      createBufferEdge(buffer.id, null, listen.id, null, {
        type: 'bufferKey',
        key: 'route',
      }),
      valid,
    ];
    const { request } = buildCompatibilityRequest({
      ...graph,
      edges,
    });
    expect(request.connections.map(({ id }) => id)).toEqual([valid.id]);
    expect(request.diagram.ops.listen).toMatchObject({ buffers: [] });
    expect(request.diagram.ops.source).toMatchObject({ next: ['buffer'] });
  });

  test('automatically selects a sole section buffer for a buffer consumer', () => {
    const source = operationNode(
      { type: 'section', template: 'test' },
      'source',
    );
    const target = operationNode(
      { type: 'listen', buffers: [], next: { builtin: 'dispose' } },
      'target',
    );
    const result = prepareConnection({
      registry: stubRegistry,
      nodeManager: new NodeManager([source, target]),
      edges: [],
      templates: {
        test: { buffers: ['storage'], ops: { storage: { type: 'buffer' } } },
      },
      connection: connect(source, target),
    });
    expect(result).toMatchObject({
      valid: true,
      edge: { type: 'buffer', data: { output: { bufferId: 'storage' } } },
    });
  });
  test('existing-edge diagnostics preserve configured ports and do not mutate the graph', () => {
    const source = operationNode(
      { type: 'section', template: 'example' },
      'source',
    );
    const target = operationNode(
      { type: 'section', template: 'example' },
      'target',
    );
    const edge = createSectionEdge(source.id, null, target.id, null, {
      output: 'right',
    });
    edge.data.input = { type: 'sectionInput', inputId: 'second' };
    const nodes = [source, target];
    const before = JSON.stringify({ nodes, edge });
    const { request } = buildCompatibilityRequest({
      registry: stubRegistry,
      nodeManager: new NodeManager(nodes),
      edges: [edge],
      templates: {
        example: {
          inputs: ['first', 'second'],
          outputs: ['left', 'right'],
          ops: {},
        },
      },
      diagramProperties: {},
    });
    expect(request.connections).toMatchObject([
      {
        id: edge.id,
        sourcePort: {
          Output: { Named: { operation: 'source', key: ['connect', 'right'] } },
        },
        targetPort: {
          Input: { named: { name: 'second', exposed_namespace: 'target' } },
        },
      },
    ]);
    expect(request.diagram.ops.source).toMatchObject({
      connect: { right: { target: 'second' } },
    });
    expect(JSON.stringify({ nodes, edge })).toBe(before);
  });
  test.each([
    {
      inputs: ['request'],
      outputs: ['response'],
      input: 'request',
      output: 'response',
    },
    {
      inputs: ['left', 'right'],
      outputs: ['yes', 'no'],
      input: '',
      output: '',
    },
  ])(
    'autoselects section ports only when unambiguous: $inputs',
    ({ inputs, outputs, input, output }) => {
      const source = operationNode(
        { type: 'section', template: 'example' },
        'source',
      );
      const target = operationNode(
        { type: 'section', template: 'example' },
        'target',
      );
      const result = prepareConnection({
        registry: stubRegistry,
        templates: { example: { inputs, outputs, ops: {} } },
        nodeManager: new NodeManager([source, target]),
        edges: [],
        connection: connect(source, target),
      });
      expect(result).toMatchObject({
        valid: true,
        edge: {
          data: {
            input: { type: 'sectionInput', inputId: input },
            output: { output },
          },
        },
      });
    },
  );

  test('buffer edges use the buffer input as an infer-only focused port', () => {
    const buffer = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'buffer' },
      'buffer',
    );
    const listen = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'listen', buffers: [], next: { builtin: 'terminate' } },
      'listen',
    );

    const result = buildConnectionPreview({
      registry: stubRegistry,
      nodeManager: new NodeManager([buffer, listen]),
      edges: [],
      templates: {},
      diagramProperties: {},
      connection: {
        source: buffer.id,
        sourceHandle: null,
        target: listen.id,
        targetHandle: null,
      },
    });

    expect(result.request).not.toBeNull();
    const connection = result.request!.connections[0];
    expect(connection.sourcePort).toBeUndefined();
    expect(connection.targetPort).toBeUndefined();
    expect(connection.focusPorts).toEqual([
      {
        Input: {
          named: {
            namespaces: [],
            exposed_namespace: null,
            name: 'buffer',
          },
        },
      },
      {
        Output: {
          Named: { namespaces: [], operation: 'listen', key: ['next'] },
        },
      },
    ]);
  });

  test('reconnect candidates replace the old edge with the same id', () => {
    const buffer = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'buffer' },
      'buffer',
    );
    const oldListen = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'listen', buffers: [], next: { builtin: 'terminate' } },
      'old_listen',
    );
    const newListen = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'listen', buffers: [], next: { builtin: 'terminate' } },
      'new_listen',
    );
    const oldEdge = createBufferEdge(buffer.id, null, oldListen.id, null, {
      type: 'bufferSeq',
      seq: 0,
    });
    oldEdge.id = 'reconnected-edge';

    const result = buildConnectionPreview({
      registry: stubRegistry,
      nodeManager: new NodeManager([buffer, oldListen, newListen]),
      edges: [oldEdge],
      templates: {},
      diagramProperties: {},
      connection: {
        source: buffer.id,
        sourceHandle: null,
        target: newListen.id,
        targetHandle: null,
      },
      edgeId: oldEdge.id,
    });

    expect(result.request).not.toBeNull();
    expect(result.request!.diagram.ops.old_listen).toMatchObject({
      type: 'listen',
      buffers: [],
    });
    expect(result.request!.diagram.ops.new_listen).toMatchObject({
      type: 'listen',
      buffers: ['buffer'],
    });
  });
});

// Retain the original API-result regression with the current advisory status.
test('compatibility checks preserve provisional unknown results', async () => {
  const response = {
    id: 'edge',
    status: 'unknown' as const,
    provisional: true,
    reason: 'more type context',
  };
  const apiClient = {
    getRegistry: jest.fn(() => of(stubRegistry)),
    postRunWorkflow: jest.fn(() => of(null)),
    checkCompatibility: jest.fn(() => of({ results: [response] })),
  } satisfies BaseApiClient;
  const results = await checkCompatibility(apiClient, {
    diagram: { version: '0.1.0', start: { builtin: 'dispose' }, ops: {} },
    connections: [{ id: 'edge' }],
  });
  expect(results.get('edge')).toEqual(response);
});
