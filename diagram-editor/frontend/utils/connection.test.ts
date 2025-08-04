import {
  createBufferEdge,
  createDefaultEdge,
  createForkResultErrEdge,
  createForkResultOkEdge,
  type DiagramEditorEdge,
} from '../edges';
import {
  createOperationNode,
  createSectionBufferNode,
  createSectionInputNode,
  createSectionOutputNode,
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
  test('"buffer" can only connect to operations that accepts a buffer', () => {
    const node = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'node', builder: 'test_builder', next: { builtin: 'dispose' } },
      'test_op_node',
    );
    const buffer = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'buffer' },
      'test_op_buffer',
    );
    const join = createOperationNode(
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

    {
      // "node" does not accept buffer
      const validEdges = getValidEdgeTypes(buffer, node);
      expect(validEdges.length).toBe(0);

      // "buffer" does not output data ("default" edge)
      const edge = createDefaultEdge(buffer.id, join.id);
      const reactFlow = new MockReactFlowAccessor([buffer, join], [edge]);
      const result = validateEdgeQuick(edge, reactFlow);
      expect(result.valid).toBe(false);
    }

    {
      const validEdges = getValidEdgeTypes(buffer, join);
      expect(validEdges.length).toBe(2);
      expect(validEdges).toContain('bufferKey');
      expect(validEdges).toContain('bufferSeq');
    }

    {
      const edge = createBufferEdge(buffer.id, join.id, {
        type: 'bufferSeq',
        seq: 0,
      });
      const reactFlow = new MockReactFlowAccessor([buffer, join], [edge]);
      const result = validateEdgeQuick(edge, reactFlow);
      expect(result.valid).toBe(true);
    }

    {
      const edge = createBufferEdge(buffer.id, join.id, {
        type: 'bufferKey',
        key: 'test',
      });
      const reactFlow = new MockReactFlowAccessor([buffer, join], [edge]);
      const result = validateEdgeQuick(edge, reactFlow);
      expect(result.valid).toBe(true);
    }
  });

  test('"buffer_access" accepts both data and buffer edges', () => {
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

  test('"join" node accepts both data and buffer edges', () => {
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

  test('"sectionInput" can only connect to operations that accepts data', () => {
    const sectionInput = createSectionInputNode(
      'test_section_input',
      'test_section_input',
      { x: 0, y: 0 },
    );
    const node = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'node', builder: 'test_builder', next: { builtin: 'dispose' } },
      'test_op_node',
    );
    const listen = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'listen', buffers: [], next: { builtin: 'dispose' } },
      'test_op_listen',
    );

    {
      const validEdges = getValidEdgeTypes(sectionInput, node);
      expect(validEdges.length).toBe(1);
      expect(validEdges).toContain('default');
    }

    {
      const validEdges = getValidEdgeTypes(sectionInput, listen);
      expect(validEdges.length).toBe(0);
    }
  });

  test('"sectionBuffer" can only connect to operations that accepts buffer', () => {
    const sectionBuffer = createSectionBufferNode(
      'test_section_buffer',
      'test_section_buffer',
      { x: 0, y: 0 },
    );
    const node = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'node', builder: 'test_builder', next: { builtin: 'dispose' } },
      'test_op_node',
    );
    const listen = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'listen', buffers: [], next: { builtin: 'dispose' } },
      'test_op_listen',
    );

    {
      const validEdges = getValidEdgeTypes(sectionBuffer, node);
      expect(validEdges.length).toBe(0);
    }

    {
      const validEdges = getValidEdgeTypes(sectionBuffer, listen);
      expect(validEdges.length).toBe(2);
      expect(validEdges).toContain('bufferKey');
      expect(validEdges).toContain('bufferSeq');
    }
  });

  test('"sectionOutput" only accepts data edges', () => {
    const sectionOutput = createSectionOutputNode('test_section_output', {
      x: 0,
      y: 0,
    });
    const node = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'node', builder: 'test_builder', next: { builtin: 'dispose' } },
      'test_op_node',
    );
    const buffer = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'buffer', buffers: [] },
      'test_op_buffer',
    );

    {
      const validEdges = getValidEdgeTypes(node, sectionOutput);
      expect(validEdges.length).toBe(1);
      expect(validEdges).toContain('default');
    }

    {
      const validEdges = getValidEdgeTypes(buffer, sectionOutput);
      expect(validEdges.length).toBe(0);
    }
  });

  test('"node" operation only allows 1 output', () => {
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

  test('"fork_clone" operation allows multiple outputs', () => {
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

  test('"fork_result" operation only allows 2 outputs', () => {
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
