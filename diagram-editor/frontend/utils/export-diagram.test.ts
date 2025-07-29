import { NodeManager } from '../node-manager';
import { START_ID } from '../nodes';
import {
  createOperationNode,
  createSectionBufferNode,
  createSectionInputNode,
  createSectionOutputNode,
} from './create-node';
import { exportDiagram, exportTemplate } from './export-diagram';
import { loadDiagramJson } from './load-diagram';
import { joinNamespaces, ROOT_NAMESPACE } from './namespace';
import testDiagram from './test-data/test-diagram.json';
import testDiagramScope from './test-data/test-diagram-scope.json';

test('export diagram', () => {
  const [_diagram, { nodes, edges }] = loadDiagramJson(
    JSON.stringify(testDiagram),
  );
  const diagram = exportDiagram(new NodeManager(nodes), edges, {});
  expect(diagram).toEqual(testDiagram);
});

test('export diagram with scope', () => {
  const [_diagram, { nodes, edges }] = loadDiagramJson(
    JSON.stringify(testDiagramScope),
  );
  let diagram = exportDiagram(new NodeManager(nodes), edges, {});
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
  diagram = exportDiagram(nodeManager, edges, {});
  expect(diagram.ops.scope.start).toBe('mul4');
});

test('export diagram with templates', () => {
  const nodes = [
    createSectionInputNode('test_input', { x: 0, y: 0 }),
    createSectionOutputNode('test_output', { x: 0, y: 0 }),
    createSectionBufferNode('test_buffer', { x: 0, y: 0 }),
    createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'node', builder: 'test', next: 'test_output' },
    ),
    createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'buffer' },
    ),
  ];
  const template = exportTemplate(new NodeManager(nodes), []);

  if (typeof template.inputs !== 'object' || Array.isArray(template.inputs)) {
    throw new Error('expected template inputs to be a mapping');
  }
  expect(template.inputs.test_input).toBe('test_input');

  expect(template.outputs?.[0]).toBe('test_output');

  if (typeof template.buffers !== 'object' || Array.isArray(template.buffers)) {
    throw new Error('expected template buffers to be a mapping');
  }
  expect(template.buffers.test_buffer).toBe('test_buffer');
});
