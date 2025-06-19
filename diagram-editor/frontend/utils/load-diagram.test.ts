import { applyNodeChanges } from '@xyflow/react';
import { START_ID } from '..';
import { autoLayout } from './auto-layout';
import { loadDiagramJson } from './load-diagram';
import testDiagram from './test-data/test-diagram.json';

test('load diagram json and auto layout', () => {
  const graph = loadDiagramJson(JSON.stringify(testDiagram));
  const nodes = applyNodeChanges(
    autoLayout(START_ID, graph.nodes, graph.edges),
    graph.nodes,
  );
  expect(nodes.length).toBe(8);
  expect(graph.edges.length).toBe(8);
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

  const mul3Buffer = map.get('mul3_buffer');
  expect(mul3Buffer!.data).toMatchObject(testDiagram.ops.mul3_buffer);
  expect(mul3Buffer!.position.x).toBe(mul3!.position.x);
  expect(mul3Buffer!.position.y).toBeGreaterThan(mul3!.position.y);

  const mul4 = map.get('mul4');
  expect(mul4!.data).toMatchObject(testDiagram.ops.mul4);
  expect(mul4!.position.x).toBeGreaterThan(forkClone!.position.x);
  expect(mul4!.position.y).toBeGreaterThan(forkClone!.position.y);

  const mul4Buffer = map.get('mul4_buffer');
  expect(mul4Buffer!.data).toMatchObject(testDiagram.ops.mul4_buffer);
  expect(mul4Buffer!.position.x).toBe(mul4!.position.x);
  expect(mul4Buffer!.position.y).toBeGreaterThan(mul4!.position.y);

  const join = map.get('join');
  expect(join!.data).toMatchObject(testDiagram.ops.join);
  expect(join!.position.x).toBeGreaterThan(mul3Buffer!.position.x);
  expect(join!.position.x).toBeLessThan(mul4Buffer!.position.x);
  expect(join!.position.y).toBeGreaterThan(mul4Buffer!.position.y);

  const terminate = map.get('builtin:terminate');
  expect(terminate).toBeDefined();
  expect(terminate!.position.x).toBe(join!.position.x);
  expect(terminate!.position.y).toBeGreaterThan(join!.position.y);
});
