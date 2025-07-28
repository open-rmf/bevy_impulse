import { NodeManager } from '../node-manager';
import { START_ID } from '../nodes';
import { exportDiagram } from './export-diagram';
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
