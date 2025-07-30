import { applyNodeChanges } from '@xyflow/react';
import { NodeManager } from '../node-manager';
import { type OperationNode, START_ID, TERMINATE_ID } from '../nodes';
import { autoLayout } from './auto-layout';
import { LAYOUT_OPTIONS } from './layout';
import { loadDiagramJson, loadTemplate } from './load-diagram';
import testDiagram from './test-data/test-diagram.json';

test('load diagram json and auto layout', () => {
  const [_diagram, graph] = loadDiagramJson(JSON.stringify(testDiagram));
  const nodes = applyNodeChanges(
    autoLayout(graph.nodes, graph.edges, LAYOUT_OPTIONS),
    graph.nodes,
  );
  expect(nodes.length).toBe(8);
  expect(graph.edges.length).toBe(8);
  const nodeManager = new NodeManager(nodes);

  const start = nodeManager.getNodeFromRootOpId(START_ID);
  expect(start).toBeDefined();

  const forkClone = nodeManager.getNodeFromRootOpId(
    'fork_clone',
  ) as OperationNode<'fork_clone'>;
  expect(forkClone!.data.op).toMatchObject(testDiagram.ops.fork_clone);
  expect(forkClone!.position.x).toBe(start!.position.x);
  expect(forkClone!.position.y).toBeGreaterThan(start!.position.y);

  const mul3 = nodeManager.getNodeFromRootOpId('mul3') as OperationNode<'node'>;
  expect(mul3!.data.op).toMatchObject(testDiagram.ops.mul3);
  expect(mul3!.position.x).toBeLessThan(forkClone!.position.x);
  expect(mul3!.position.y).toBeGreaterThan(forkClone!.position.y);

  const mul3Buffer = nodeManager.getNodeFromRootOpId(
    'mul3_buffer',
  ) as OperationNode<'buffer'>;
  expect(mul3Buffer!.data.op).toMatchObject(testDiagram.ops.mul3_buffer);
  expect(mul3Buffer!.position.x).toBe(mul3!.position.x);
  expect(mul3Buffer!.position.y).toBeGreaterThan(mul3!.position.y);

  const mul4 = nodeManager.getNodeFromRootOpId('mul4') as OperationNode<'node'>;
  expect(mul4!.data.op).toMatchObject(testDiagram.ops.mul4);
  expect(mul4!.position.x).toBeGreaterThan(forkClone!.position.x);
  expect(mul4!.position.y).toBeGreaterThan(forkClone!.position.y);

  const mul4Buffer = nodeManager.getNodeFromRootOpId(
    'mul4_buffer',
  ) as OperationNode<'buffer'>;
  expect(mul4Buffer!.data.op).toMatchObject(testDiagram.ops.mul4_buffer);
  expect(mul4Buffer!.position.x).toBe(mul4!.position.x);
  expect(mul4Buffer!.position.y).toBeGreaterThan(mul4!.position.y);

  const join = nodeManager.getNodeFromRootOpId('join') as OperationNode<'join'>;
  expect(join!.data.op).toMatchObject(testDiagram.ops.join);
  expect(join!.position.x).toBeGreaterThan(mul3Buffer!.position.x);
  expect(join!.position.x).toBeLessThan(mul4Buffer!.position.x);
  expect(join!.position.y).toBeGreaterThan(mul4Buffer!.position.y);

  const terminate = nodeManager.getNodeFromRootOpId(TERMINATE_ID);
  expect(terminate).toBeDefined();
  expect(terminate!.position.x).toBe(join!.position.x);
  expect(terminate!.position.y).toBeGreaterThan(join!.position.y);
});

test('load template', () => {
  const graph = loadTemplate({
    inputs: {
      remapped_input: 'target_input',
    },
    outputs: ['output'],
    buffers: {},
    ops: {
      target_input: {
        type: 'node',
        builder: 'add',
        next: 'output',
      },
    },
  });

  expect(graph.nodes.length).toBe(3);
  expect(graph.edges.length).toBe(2);
});
