import {
  createBufferEdge,
  createDefaultEdge,
  createForkResultErrEdge,
  createForkResultOkEdge,
  type DiagramEditorEdge,
} from '../edges';
import { NodeManager } from '../node-manager';
import {
  createOperationNode,
  createSectionBufferNode,
  createSectionInputNode,
  createSectionOutputNode,
  createTerminateNode,
} from '../nodes';
import {
  getValidEdgeTypes,
  validateEdgeQuick,
  validateEdgeSimple,
} from './connection';
import { ROOT_NAMESPACE } from './namespace';

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
      const validEdges = getValidEdgeTypes(buffer, null, node, null);
      expect(validEdges.length).toBe(0);

      // "buffer" does not output data ("default" edge)
      const edge = createDefaultEdge(buffer.id, null, join.id, null);
      const nodeManager = new NodeManager([buffer, join]);
      const result = validateEdgeQuick(edge, nodeManager);
      expect(result.valid).toBe(false);
    }

    {
      const validEdges = getValidEdgeTypes(buffer, null, join, null);
      expect(validEdges.length).toBe(1);
      expect(validEdges).toContain('buffer');
    }

    {
      const edge = createBufferEdge(buffer.id, null, join.id, null, {
        type: 'bufferSeq',
        seq: 0,
      });
      const nodeManager = new NodeManager([buffer, join]);
      const result = validateEdgeQuick(edge, nodeManager);
      expect(result.valid).toBe(true);
    }

    {
      const edge = createBufferEdge(buffer.id, null, join.id, null, {
        type: 'bufferKey',
        key: 'test',
      });
      const nodeManager = new NodeManager([buffer, join]);
      const result = validateEdgeQuick(edge, nodeManager);
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
      const validEdges = getValidEdgeTypes(
        nodeNode,
        null,
        bufferAccessNode,
        null,
      );
      expect(validEdges.length).toBe(2);
      expect(validEdges).toContain('default');
      expect(validEdges).toContain('streamOut');
    }
    {
      const validEdges = getValidEdgeTypes(
        bufferNode,
        null,
        bufferAccessNode,
        null,
      );
      expect(validEdges.length).toBe(1);
      expect(validEdges).toContain('buffer');
    }
  });

  test('"join" node only accepts buffer edges', () => {
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
        const validEdges = getValidEdgeTypes(nodeNode, null, targetNode, null);
        expect(validEdges.length).toBe(0);
      }
      {
        const validEdges = getValidEdgeTypes(
          bufferNode,
          null,
          targetNode,
          null,
        );
        expect(validEdges.length).toBe(1);
        expect(validEdges).toContain('buffer');
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
      const validEdges = getValidEdgeTypes(sectionInput, null, node, null);
      expect(validEdges.length).toBe(1);
      expect(validEdges).toContain('default');
    }

    {
      const validEdges = getValidEdgeTypes(sectionInput, null, listen, null);
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
      const validEdges = getValidEdgeTypes(sectionBuffer, null, node, null);
      expect(validEdges.length).toBe(0);
    }

    {
      const validEdges = getValidEdgeTypes(sectionBuffer, null, listen, null);
      expect(validEdges.length).toBe(1);
      expect(validEdges).toContain('buffer');
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
      const validEdges = getValidEdgeTypes(node, null, sectionOutput, null);
      expect(validEdges.length).toBe(2);
      expect(validEdges).toContain('default');
      expect(validEdges).toContain('streamOut');
    }

    {
      const validEdges = getValidEdgeTypes(buffer, null, sectionOutput, null);
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

    const existingEdge = createDefaultEdge(
      nodeNode.id,
      null,
      forkCloneNode.id,
      null,
    );
    const nodeManager = new NodeManager([
      nodeNode,
      forkCloneNode,
      forkCloneNode2,
    ]);
    const edges = [existingEdge];
    {
      const result = validateEdgeSimple(existingEdge, nodeManager, edges);
      expect(result.valid).toBe(true);
    }
    {
      const newEdge = createDefaultEdge(
        nodeNode.id,
        null,
        forkCloneNode2.id,
        null,
      );
      const result = validateEdgeSimple(newEdge, nodeManager, edges);
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
      createDefaultEdge(forkCloneNode.id, null, terminateNode.id, null),
      createDefaultEdge(forkCloneNode.id, null, terminateNode.id, null),
    ];
    const nodeManager = new NodeManager([forkCloneNode, terminateNode]);

    {
      const newEdge = createDefaultEdge(
        forkCloneNode.id,
        null,
        terminateNode.id,
        null,
      );
      const result = validateEdgeSimple(newEdge, nodeManager, edges);
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
    const nodeManager = new NodeManager([forkResultNode, terminateNode]);

    {
      const existingEdges = [
        createForkResultOkEdge(forkResultNode.id, null, terminateNode.id, null),
      ];
      const newEdge = createForkResultErrEdge(
        forkResultNode.id,
        null,
        terminateNode.id,
        null,
      );
      const result = validateEdgeSimple(newEdge, nodeManager, existingEdges);
      expect(result.valid).toBe(true);
    }

    {
      const existingEdges = [
        createForkResultOkEdge(forkResultNode.id, null, terminateNode.id, null),
        createForkResultErrEdge(
          forkResultNode.id,
          null,
          terminateNode.id,
          null,
        ),
      ];
      const newEdge = createForkResultErrEdge(
        forkResultNode.id,
        null,
        terminateNode.id,
        null,
      );
      const result = validateEdgeSimple(newEdge, nodeManager, existingEdges);
      expect(result.valid).toBe(false);
    }
  });

  test('buffer edges connecting to a section must have "sectionBuffer" input', () => {
    const bufferNode = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      {
        type: 'buffer',
      },
      'test_op_buffer',
    );
    const sectionNode = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'section', builder: 'test_section' },
      'test_op_section',
    );
    const nodeManager = new NodeManager([bufferNode, sectionNode]);

    {
      const edge = createBufferEdge(bufferNode.id, null, sectionNode.id, null, {
        type: 'bufferSeq',
        seq: 0,
      });
      const result = validateEdgeSimple(edge, nodeManager, []);
      expect(result.valid).toBe(false);
    }
    {
      const edge = createBufferEdge(bufferNode.id, null, sectionNode.id, null, {
        type: 'sectionBuffer',
        inputId: 'test',
      });
      const result = validateEdgeSimple(edge, nodeManager, []);
      expect(result.valid).toBe(true);
    }
  });

  test('data edges connecting to a section must have "sectionInput" input', () => {
    const nodeNode = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      {
        type: 'node',
        builder: 'test_builder',
        next: { builtin: 'dispose' },
      },
      'test_op_node',
    );
    const sectionNode = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'section', builder: 'test_section' },
      'test_op_section',
    );

    {
      const nodeManager = new NodeManager([nodeNode, sectionNode]);
      const edges: DiagramEditorEdge[] = [];
      const edge = createDefaultEdge(nodeNode.id, null, sectionNode.id, null);
      const result = validateEdgeSimple(edge, nodeManager, edges);
      expect(result.valid).toBe(false);
    }
    {
      const nodeManager = new NodeManager([nodeNode, sectionNode]);
      const edges: DiagramEditorEdge[] = [];
      const edge = createDefaultEdge(nodeNode.id, null, sectionNode.id, null, {
        type: 'sectionInput',
        inputId: 'test',
      });
      const result = validateEdgeSimple(edge, nodeManager, edges);
      expect(result.valid).toBe(true);
    }
  });
});
