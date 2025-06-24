import { applyNodeChanges } from '@xyflow/react';
import { NodeManager } from '../node-manager';
import { START_ID, TERMINATE_ID } from '../nodes';
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
  const nodeManager = new NodeManager(nodes);

  const start = nodeManager.getNode(START_ID);
  expect(start).toBeDefined();
  expect(start!.position).toStrictEqual({ x: 0, y: 0 });

  const forkClone = nodeManager.getNodeFromRootOpId('fork_clone');
  expect(forkClone!.data.op).toMatchObject(testDiagram.ops.fork_clone);
  expect(forkClone!.position.x).toBe(start!.position.x);
  expect(forkClone!.position.y).toBeGreaterThan(start!.position.y);

  const mul3 = nodeManager.getNodeFromRootOpId('mul3');
  expect(mul3!.data.op).toMatchObject(testDiagram.ops.mul3);
  expect(mul3!.position.x).toBeLessThan(forkClone!.position.x);
  expect(mul3!.position.y).toBeGreaterThan(forkClone!.position.y);

  const mul3Buffer = nodeManager.getNodeFromRootOpId('mul3_buffer');
  expect(mul3Buffer!.data.op).toMatchObject(testDiagram.ops.mul3_buffer);
  expect(mul3Buffer!.position.x).toBe(mul3!.position.x);
  expect(mul3Buffer!.position.y).toBeGreaterThan(mul3!.position.y);

  const mul4 = nodeManager.getNodeFromRootOpId('mul4');
  expect(mul4!.data.op).toMatchObject(testDiagram.ops.mul4);
  expect(mul4!.position.x).toBeGreaterThan(forkClone!.position.x);
  expect(mul4!.position.y).toBeGreaterThan(forkClone!.position.y);

  const mul4Buffer = nodeManager.getNodeFromRootOpId('mul4_buffer');
  expect(mul4Buffer!.data.op).toMatchObject(testDiagram.ops.mul4_buffer);
  expect(mul4Buffer!.position.x).toBe(mul4!.position.x);
  expect(mul4Buffer!.position.y).toBeGreaterThan(mul4!.position.y);

  const join = nodeManager.getNodeFromRootOpId('join');
  expect(join!.data.op).toMatchObject(testDiagram.ops.join);
  expect(join!.position.x).toBeGreaterThan(mul3Buffer!.position.x);
  expect(join!.position.x).toBeLessThan(mul4Buffer!.position.x);
  expect(join!.position.y).toBeGreaterThan(mul4Buffer!.position.y);

  const terminate = nodeManager.getNode(TERMINATE_ID);
  expect(terminate).toBeDefined();
  expect(terminate!.position.x).toBe(join!.position.x);
  expect(terminate!.position.y).toBeGreaterThan(join!.position.y);
});
