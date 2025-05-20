import { loadDiagramJson } from './load-diagram-json';
import testDiagram from './test-data/multiply3.json';

test('load diagram json', () => {
  const nodes = loadDiagramJson(JSON.stringify(testDiagram));
  expect(nodes.length).toBe(1);
  expect(nodes[0].id).toBe('mul3');
  expect(nodes[0].data).toStrictEqual(testDiagram.ops.mul3);
});
