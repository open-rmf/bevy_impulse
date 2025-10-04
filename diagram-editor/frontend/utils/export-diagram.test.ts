import { v4 as uuidv4 } from 'uuid';
import type { DiagramEditorEdge } from '../edges';
import { NodeManager } from '../node-manager';
import {
  createOperationNode,
  createSectionBufferNode,
  createSectionInputNode,
  createSectionOutputNode,
  type DiagramEditorNode,
  START_ID,
} from '../nodes';
import { exportDiagram, exportTemplate } from './export-diagram';
import { loadDiagramJson } from './load-diagram';
import { joinNamespaces, ROOT_NAMESPACE } from './namespace';
import testDiagram from './test-data/test-diagram.json';
import testDiagramScope from './test-data/test-diagram-scope.json';
import type { DiagramElementRegistry } from '../types/api';

const stubRegistry: DiagramElementRegistry = {
  messages: {},
  nodes: {},
  schemas: {},
  sections: {},
  trace_supported: false,
};

test('export diagram', () => {
  const [_diagram, { nodes, edges }] = loadDiagramJson(
    JSON.stringify(testDiagram),
  );
  const diagram = exportDiagram(
    stubRegistry,
    new NodeManager(nodes),
    edges,
    {},
  );
  expect(diagram).toEqual(testDiagram);
});

test('export diagram with scope', () => {
  const [_diagram, { nodes, edges }] = loadDiagramJson(
    JSON.stringify(testDiagramScope),
  );
  let diagram = exportDiagram(stubRegistry, new NodeManager(nodes), edges, {});
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
  diagram = exportDiagram(stubRegistry, nodeManager, edges, {});
  expect(diagram.ops.scope.start).toBe('mul4');
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
