import { loadDiagramJson } from './load-diagram-json';
import testDiagram from './test-data/test-diagram.json';

test('load diagram json', () => {
  const { nodes, edges } = loadDiagramJson(JSON.stringify(testDiagram));
  expect(nodes.length).toBe(5);
  expect(edges.length).toBe(5);
  const map = new Map(nodes.map((node) => [node.id, node]));

  const start = map.get('builtin:start');
  expect(start).toBeDefined();
  expect(start!.position).toStrictEqual({ x: 0, y: 0 });

  const forkClone = map.get('fork_clone');
  expect(forkClone!.data).toMatchObject(testDiagram.ops.fork_clone);
  expect(forkClone!.position.x).toBe(start!.position.x);
  expect(forkClone!.position.y).toBeGreaterThan(start!.position.y);

  const mul3 = map.get('mul3');
  expect(mul3!.data).toMatchObject(testDiagram.ops.mul3);
  expect(mul3!.position.x).toBeLessThan(forkClone!.position.x);
  expect(mul3!.position.y).toBeGreaterThan(forkClone!.position.y);

  const mul4 = map.get('mul4');
  expect(mul4!.data).toMatchObject(testDiagram.ops.mul4);
  expect(mul4!.position.x).toBeGreaterThan(forkClone!.position.x);
  expect(mul4!.position.y).toBeGreaterThan(forkClone!.position.y);

  const terminate = map.get('builtin:terminate');
  expect(terminate).toBeDefined();
  expect(terminate!.position.x).toBeGreaterThan(mul3!.position.x);
  expect(terminate!.position.x).toBeLessThan(mul4!.position.x);
  expect(terminate!.position.y).toBeGreaterThan(mul4!.position.y);
});
