import { NodeManager } from '../node-manager';
import { exportDiagram } from './export-diagram';
import { loadDiagramJson } from './load-diagram';
import testDiagram from './test-data/test-diagram.json';

test('export diagram', () => {
  const { nodes, edges } = loadDiagramJson(JSON.stringify(testDiagram));
  const diagram = exportDiagram(new NodeManager(nodes), edges);
  expect(diagram).toEqual(testDiagram);
});
