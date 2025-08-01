import {
  createBufferKeyEdge,
  createBufferSeqEdge,
  createDefaultEdge,
  createForkResultErrEdge,
  createForkResultOkEdge,
  type DiagramEditorEdge,
} from '../edges';
import {
  createOperationNode,
  createTerminateNode,
  type DiagramEditorNode,
} from '../nodes';
import {
  getValidEdgeTypes,
  type NodesAndEdgesAccessor,
  validateEdgeQuick,
  validateEdgeSimple,
} from './connection';
import { ROOT_NAMESPACE } from './namespace';

class MockReactFlowAccessor implements NodesAndEdgesAccessor {
  nodesMap: Record<string, DiagramEditorNode>;
  edgesMap: Record<string, DiagramEditorEdge>;

  constructor(nodes: DiagramEditorNode[], edges: DiagramEditorEdge[]) {
    this.nodesMap = nodes.reduce(
      (map, node) => {
        map[node.id] = node;
        return map;
      },
      {} as Record<string, DiagramEditorNode>,
    );
    this.edgesMap = edges.reduce(
      (map, edge) => {
        map[edge.id] = edge;
        return map;
      },
      {} as Record<string, DiagramEditorEdge>,
    );
  }

  getNode(id: string): DiagramEditorNode | undefined {
    return this.nodesMap[id];
  }

  getNodes(): DiagramEditorNode[] {
    return Object.values(this.nodesMap);
  }

  getEdges(): DiagramEditorEdge[] {
    return Object.values(this.edgesMap);
  }
}

describe('validate edges', () => {
  test('buffer->node is invalid', () => {
    const sourceNode = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'buffer' },
      'test_op_buffer',
    );
    const targetNode = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'node', builder: 'test_builder', next: { builtin: 'dispose' } },
      'test_op_node',
    );

    const validEdges = getValidEdgeTypes(sourceNode, targetNode);
    expect(validEdges.length).toBe(0);

    const edge = createDefaultEdge(sourceNode.id, targetNode.id);
    const reactFlow = new MockReactFlowAccessor(
      [sourceNode, targetNode],
      [edge],
    );
    const result = validateEdgeQuick(edge, reactFlow);
    expect(result.valid).toBe(false);
  });

  test('buffer->join is valid only for buffer edges', () => {
    const sourceNode = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'buffer' },
      'test_op_buffer',
    );
    const targetNode = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      {
        type: 'join',
        buffers: [],
        next: { builtin: 'dispose' },
      },
      'test_op_join',
    );

    const validEdges = getValidEdgeTypes(sourceNode, targetNode);
    expect(validEdges.length).toBe(2);
    expect(validEdges).toContain('bufferKey');
    expect(validEdges).toContain('bufferSeq');

    {
      const edge = createBufferSeqEdge(sourceNode.id, targetNode.id, {
        seq: 0,
      });
      const reactFlow = new MockReactFlowAccessor(
        [sourceNode, targetNode],
        [edge],
      );
      const result = validateEdgeQuick(edge, reactFlow);
      expect(result.valid).toBe(true);
    }

    {
      const edge = createBufferKeyEdge(sourceNode.id, targetNode.id, {
        key: 'test',
      });
      const reactFlow = new MockReactFlowAccessor(
        [sourceNode, targetNode],
        [edge],
      );
      const result = validateEdgeQuick(edge, reactFlow);
      expect(result.valid).toBe(true);
    }

    {
      const edge = createDefaultEdge(sourceNode.id, targetNode.id);
      const reactFlow = new MockReactFlowAccessor(
        [sourceNode, targetNode],
        [edge],
      );
      const result = validateEdgeQuick(edge, reactFlow);
      expect(result.valid).toBe(false);
    }
  });

  test('node->buffer_access and buffer->buffer_access are valid', () => {
    const nodeNode = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'node', builder: 'test_builder', next: { builtin: 'dispose' } },
      'test_op_node',
    );
    const bufferNode = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'buffer' },
      'test_op_buffer',
    );
    const bufferAccessNode = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'buffer_access', buffers: [], next: { builtin: 'dispose' } },
      'test_op_buffer_access',
    );

    {
      const validEdges = getValidEdgeTypes(nodeNode, bufferAccessNode);
      expect(validEdges.length).toBe(1);
      expect(validEdges).toContain('default');
    }
    {
      const validEdges = getValidEdgeTypes(bufferNode, bufferAccessNode);
      expect(validEdges.length).toBe(2);
      expect(validEdges).toContain('bufferKey');
      expect(validEdges).toContain('bufferSeq');
    }
  });

  test('node->join and buffer->join are valid', () => {
    const nodeNode = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'node', builder: 'test_builder', next: { builtin: 'dispose' } },
      'test_op_node',
    );
    const bufferNode = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'buffer' },
      'test_op_buffer',
    );
    const joinNode = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'join', buffers: [], next: { builtin: 'dispose' } },
      'test_op_join',
    );
    const serializedJoinNode = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'serialized_join', buffers: [], next: { builtin: 'dispose' } },
      'test_op_serialized_join',
    );

    for (const targetNode of [joinNode, serializedJoinNode]) {
      {
        const validEdges = getValidEdgeTypes(nodeNode, targetNode);
        expect(validEdges.length).toBe(1);
        expect(validEdges).toContain('default');
      }
      {
        const validEdges = getValidEdgeTypes(bufferNode, targetNode);
        expect(validEdges.length).toBe(2);
        expect(validEdges).toContain('bufferKey');
        expect(validEdges).toContain('bufferSeq');
      }
    }
  });

  test('node operation only allows 1 output', () => {
    const nodeNode = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'node', builder: 'test_builder', next: { builtin: 'dispose' } },
      'test_op_node',
    );
    const forkCloneNode = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'fork_clone', next: [] },
      'test_fork_clone',
    );
    const forkCloneNode2 = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'fork_clone', next: [] },
      'test_fork_clone2',
    );

    const existingEdge = createDefaultEdge(nodeNode.id, forkCloneNode.id);
    const reactFlow = new MockReactFlowAccessor(
      [nodeNode, forkCloneNode],
      [existingEdge],
    );
    {
      const result = validateEdgeSimple(existingEdge, reactFlow);
      expect(result.valid).toBe(true);
    }
    {
      const newEdge = createDefaultEdge(nodeNode.id, forkCloneNode2.id);
      const result = validateEdgeSimple(newEdge, reactFlow);
      expect(result.valid).toBe(false);
    }
  });

  test('fork clone operation allows multiple outputs', () => {
    const forkCloneNode = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'fork_clone', next: [] },
      'test_fork_clone',
    );
    const terminateNode = createTerminateNode(ROOT_NAMESPACE, { x: 0, y: 0 });

    const edges = [
      createDefaultEdge(forkCloneNode.id, terminateNode.id),
      createDefaultEdge(forkCloneNode.id, terminateNode.id),
    ];
    const reactFlow = new MockReactFlowAccessor(
      [forkCloneNode, terminateNode],
      edges,
    );

    {
      const newEdge = createDefaultEdge(forkCloneNode.id, terminateNode.id);
      const result = validateEdgeSimple(newEdge, reactFlow);
      expect(result.valid).toBe(true);
    }
  });

  test('fork result operation only allows 2 outputs', () => {
    const forkResultNode = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      {
        type: 'fork_result',
        ok: { builtin: 'dispose' },
        err: { builtin: 'dispose' },
      },
      'test_fork_result',
    );
    const terminateNode = createTerminateNode(ROOT_NAMESPACE, { x: 0, y: 0 });

    {
      const existingEdges = [
        createForkResultOkEdge(forkResultNode.id, terminateNode.id),
      ];
      const reactFlow = new MockReactFlowAccessor(
        [forkResultNode, terminateNode],
        existingEdges,
      );
      const newEdge = createForkResultErrEdge(
        forkResultNode.id,
        terminateNode.id,
      );
      const result = validateEdgeSimple(newEdge, reactFlow);
      expect(result.valid).toBe(true);
    }

    {
      const existingEdges = [
        createForkResultOkEdge(forkResultNode.id, terminateNode.id),
        createForkResultErrEdge(forkResultNode.id, terminateNode.id),
      ];
      const reactFlow = new MockReactFlowAccessor(
        [forkResultNode, terminateNode],
        existingEdges,
      );
      const newEdge = createForkResultErrEdge(
        forkResultNode.id,
        terminateNode.id,
      );
      const result = validateEdgeSimple(newEdge, reactFlow);
      expect(result.valid).toBe(false);
    }
  });
});
