import { v4 as uuidv4 } from 'uuid';
import { createDefaultEdge, type DiagramEditorEdge } from '../edges';
import { NodeManager } from '../node-manager';
import {
  createOperationNode,
  createSectionBufferNode,
  createSectionInputNode,
  createSectionOutputNode,
  type DiagramEditorNode,
  START_ID,
} from '../nodes';
import type { DiagramElementMetadata } from '../types/api';
import { exportDiagram, exportTemplate } from './export-diagram';
import { loadDiagramJson } from './load-diagram';
import { joinNamespaces, ROOT_NAMESPACE } from './namespace';
import testDiagram from './test-data/test-diagram.json';
import testDiagramScope from './test-data/test-diagram-scope.json';

function operationNode(
  op: Parameters<typeof createOperationNode>[3],
  id: string,
) {
  return createOperationNode(ROOT_NAMESPACE, undefined, { x: 0, y: 0 }, op, id);
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

async function loadSource(source: unknown) {
  const [, { graph }] = await loadDiagramJson(JSON.stringify(source));
  return { nodeManager: new NodeManager(graph.nodes), edges: graph.edges };
}

test('export diagram', async () => {
  const [
    _diagram,
    {
      graph: { nodes, edges },
    },
  ] = await loadDiagramJson(JSON.stringify(testDiagram));
  const diagram = exportDiagram(
    stubRegistry,
    new NodeManager(nodes),
    edges,
    {},
    {},
  );
  expect(diagram).toEqual(testDiagram);
});

test('exporting new connections does not rewrite the live nodes', () => {
  const source = operationNode(
    { type: 'node', builder: 'source', next: { builtin: 'dispose' } },
    'source',
  );
  const target = operationNode({ type: 'buffer' }, 'target');
  const nodes = [source, target];
  const before = JSON.stringify(nodes);
  const edges = [createDefaultEdge(source.id, null, target.id, null)];
  const manager = new NodeManager(nodes);
  expect(
    exportDiagram(stubRegistry, manager, edges, {}, {}).ops.source,
  ).toMatchObject({ next: 'target' });
  expect(JSON.stringify(nodes)).toBe(before);
  expect(
    exportTemplate(stubRegistry, manager, edges).ops?.source,
  ).toMatchObject({ next: 'target' });
  expect(JSON.stringify(nodes)).toBe(before);
});

test('export unzip with an unused first output', async () => {
  const unzip = {
    type: 'unzip',
    next: [{ builtin: 'dispose' }, { builtin: 'terminate' }],
  };
  const { nodeManager, edges } = await loadSource({
    version: '0.1.0',
    start: 'unzip',
    ops: { unzip },
  });
  const diagram = exportDiagram(stubRegistry, nodeManager, edges, {}, {});
  expect(diagram.ops.unzip).toEqual(unzip);
});

test('round-trips an exposed section buffer and its clone selection', async () => {
  const ops = {
    section: { type: 'section', builder: 'storage' },
    join: {
      type: 'join',
      buffers: { stored: { section: 'cache' } },
      clone: ['stored'],
      next: { builtin: 'dispose' },
    },
  };
  const { nodeManager, edges } = await loadSource({
    version: '0.1.0',
    start: { builtin: 'dispose' },
    ops,
  });
  expect(
    exportDiagram(stubRegistry, nodeManager, edges, {}, {}).ops.join,
  ).toEqual(ops.join);
});

test('export diagram with scope', async () => {
  const [
    _diagram,
    {
      graph: { nodes, edges },
    },
  ] = await loadDiagramJson(JSON.stringify(testDiagramScope));
  let diagram = exportDiagram(
    stubRegistry,
    new NodeManager(nodes),
    edges,
    {},
    {},
  );
  expect(diagram).toEqual(testDiagramScope);

  const nodeManager = new NodeManager(nodes);
  const scopeStartNode = nodeManager.getNodeFromNamespaceOpId(
    joinNamespaces(ROOT_NAMESPACE, 'scope'),
    START_ID,
  );
  if (!scopeStartNode) {
    fail('scope start node not found');
  }
  const scopeStartEdge = edges.find(
    (edge) => edge.source === scopeStartNode.id,
  );
  if (!scopeStartEdge) {
    fail('scope start edge not found');
  }

  scopeStartEdge.target = nodeManager.getNodeFromNamespaceOpId(
    joinNamespaces(ROOT_NAMESPACE, 'scope'),
    'mul4',
  ).id;
  diagram = exportDiagram(stubRegistry, nodeManager, edges, {}, {});
  expect(diagram.ops.scope.start).toBe('mul4');
});

test('scope stream export preserves the main destination', async () => {
  const source = {
    version: '0.1.0',
    start: 'scope',
    ops: {
      scope: {
        type: 'scope',
        start: 'work',
        ops: {
          work: {
            type: 'node',
            builder: 'work',
            next: { builtin: 'terminate' },
          },
        },
        next: 'result',
        stream_out: { progress: 'log' },
      },
      result: {
        type: 'node',
        builder: 'result',
        next: { builtin: 'terminate' },
      },
      log: { type: 'node', builder: 'log', next: { builtin: 'dispose' } },
    },
  };
  const { nodeManager, edges } = await loadSource(source);
  const exported = exportDiagram(stubRegistry, nodeManager, edges, {}, {});
  expect(exported.ops.scope).toEqual(source.ops.scope);
});

test('export diagram with templates', () => {
  const nodes: DiagramEditorNode[] = [
    createSectionInputNode('test_input', { builti: 'dispose' }, { x: 0, y: 0 }),
    createSectionOutputNode('test_output', { x: 0, y: 0 }),
    createSectionBufferNode(
      'test_buffer',
      { builtin: 'dispose' },
      { x: 0, y: 0 },
    ),
    createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'node', builder: 'test_builder', next: { builtin: 'dispose' } },
      'test_op_node',
    ),
    createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'buffer' },
      'test_op_buffer',
    ),
  ];
  const edges: DiagramEditorEdge[] = [
    {
      id: uuidv4(),
      type: 'default',
      source: nodes[0].id,
      target: nodes[3].id,
      data: { output: {}, input: { type: 'default' } },
    },
    {
      id: uuidv4(),
      type: 'default',
      source: nodes[2].id,
      target: nodes[4].id,
      data: { output: {}, input: { type: 'default' } },
    },
    {
      id: uuidv4(),
      type: 'default',
      source: nodes[3].id,
      target: nodes[1].id,
      data: { output: {}, input: { type: 'default' } },
    },
  ];
  const template = exportTemplate(stubRegistry, new NodeManager(nodes), edges);

  if (typeof template.inputs !== 'object' || Array.isArray(template.inputs)) {
    throw new Error('expected template inputs to be a mapping');
  }
  expect(template.inputs.test_input).toBe('test_op_node');

  expect(template.outputs?.[0]).toBe('test_output');

  if (typeof template.buffers !== 'object' || Array.isArray(template.buffers)) {
    throw new Error('expected template buffers to be a mapping');
  }
  expect(template.buffers.test_buffer).toBe('test_op_buffer');
});

test('round-trips named section destinations from START and an operation', async () => {
  const source = {
    version: '0.1.0',
    start: { section: 'request' },
    ops: {
      section: {
        type: 'section',
        builder: 'processor',
        connect: { response: 'node' },
      },
      node: { type: 'node', builder: 'worker', next: { section: 'request' } },
    },
  };
  const { nodeManager, edges } = await loadSource(source);
  expect(exportDiagram(stubRegistry, nodeManager, edges, {}, {})).toMatchObject(
    source,
  );
});

test('round-trips repeated fork-clone branches without collapsing their output indexes', async () => {
  const ops = {
    fork: { type: 'fork_clone', next: ['target', 'target'] },
    target: { type: 'buffer' },
  };
  const { nodeManager, edges } = await loadSource({
    version: '0.1.0',
    start: 'fork',
    ops,
  });
  expect(
    exportDiagram(stubRegistry, nodeManager, edges, {}, {}).ops.fork,
  ).toEqual(ops.fork);
});
