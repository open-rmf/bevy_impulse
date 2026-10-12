import {
  createBufferEdge,
  createDefaultEdge,
  createForkResultErrEdge,
  createForkResultOkEdge,
  createSectionEdge,
  type DiagramEditorEdge,
} from '../edges';
import { HandleId } from '../handles';
import { NodeManager } from '../node-manager';
import {
  createOperationNode,
  createSectionBufferNode,
  createSectionInputNode,
  createSectionOutputNode,
  createTerminateNode,
} from '../nodes';
import {
  createConnectionFromDraggedHandle,
  createEdgeFromConnection,
  getValidEdgeTypes,
  validateConnectionSimple,
  validateDraggedHandlePair,
  validateEdgeQuick,
  validateEdgeSimple,
  validateSourceOutputCapacity,
} from './connection';
import { ROOT_NAMESPACE } from './namespace';

function operationNode(
  op: Parameters<typeof createOperationNode>[3],
  id: string,
) {
  return createOperationNode(ROOT_NAMESPACE, undefined, { x: 0, y: 0 }, op, id);
}

describe('connection helpers', () => {
  test('reconnect preserves a named output and a second wire cannot reuse it', () => {
    const source = operationNode(
      { type: 'section', template: 'example' },
      'source',
    );
    const target = operationNode({ type: 'buffer' }, 'target');
    const edge = createSectionEdge(source.id, null, target.id, null, {
      output: 'response',
    });
    const manager = new NodeManager([source, target]);
    expect(
      createEdgeFromConnection(
        {
          source: source.id,
          target: target.id,
          sourceHandle: null,
          targetHandle: null,
        },
        manager,
        [edge],
        edge.id,
      ),
    ).toMatchObject({
      valid: true,
      edge: {
        id: edge.id,
        type: 'section',
        data: { output: { output: 'response' } },
      },
    });
    const duplicate = { ...edge, id: 'duplicate' };
    expect(
      validateEdgeSimple(duplicate, manager, [edge, duplicate]).valid,
    ).toBe(false);
  });

  test('normalizes a drag that starts from a source handle', () => {
    const connection = createConnectionFromDraggedHandle({
      fromNodeId: 'source-node',
      fromHandleId: 'out',
      fromHandleType: 'source',
      otherNodeId: 'target-node',
      otherHandleId: 'in',
    });
    expect(connection).toEqual({
      source: 'source-node',
      sourceHandle: 'out',
      target: 'target-node',
      targetHandle: 'in',
    });
  });

  test('normalizes a drag that starts from a target handle', () => {
    const connection = createConnectionFromDraggedHandle({
      fromNodeId: 'target-node',
      fromHandleId: 'in',
      fromHandleType: 'target',
      otherNodeId: 'source-node',
      otherHandleId: 'out',
    });
    expect(connection).toEqual({
      source: 'source-node',
      sourceHandle: 'out',
      target: 'target-node',
      targetHandle: 'in',
    });
  });

  test('rejects dragged handle pairs with the same direction', () => {
    expect(
      validateDraggedHandlePair({
        fromHandleType: 'source',
        otherHandleType: 'source',
      }),
    ).toEqual({
      valid: false,
      error: 'Cannot connect an output to another output',
    });
    expect(
      validateDraggedHandlePair({
        fromHandleType: 'target',
        otherHandleType: 'target',
      }),
    ).toEqual({
      valid: false,
      error: 'Cannot connect an input to another input',
    });
    expect(
      validateDraggedHandlePair({
        fromHandleType: 'source',
        otherHandleType: 'target',
      }),
    ).toEqual({ valid: true });
  });

  test('detects single-output capacity conflicts', () => {
    const source = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'node', builder: 'test_builder', next: { builtin: 'dispose' } },
      'source',
    );
    const firstTarget = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'node', builder: 'test_builder', next: { builtin: 'dispose' } },
      'first_target',
    );
    const secondTarget = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'node', builder: 'test_builder', next: { builtin: 'dispose' } },
      'second_target',
    );
    const existingEdge = createDefaultEdge(
      source.id,
      null,
      firstTarget.id,
      null,
    );

    expect(validateSourceOutputCapacity(source, null, [])).toEqual({
      valid: true,
    });
    expect(
      validateSourceOutputCapacity(source, null, [
        existingEdge as DiagramEditorEdge,
      ]),
    ).toEqual({
      valid: false,
      error: 'This output can only be connected to one input',
    });
    expect(
      validateSourceOutputCapacity(
        source,
        null,
        [existingEdge as DiagramEditorEdge],
        existingEdge.id,
      ),
    ).toEqual({ valid: true });

    // Keep the second target live so this test also documents that the capacity
    // check only depends on source edges, not target nodes.
    expect(secondTarget.id).toBeTruthy();
  });

  test('creates keyed buffer edge for targets that already use keyed buffers', () => {
    const buffer = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'buffer' },
      'buffer_three',
    );
    const join = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      {
        type: 'join',
        buffers: {
          route: 'buffer_one',
          station: 'buffer_two',
        },
        next: { builtin: 'dispose' },
      },
      'join',
    );
    const nodeManager = new NodeManager([buffer, join]);

    const result = createEdgeFromConnection(
      {
        source: buffer.id,
        sourceHandle: null,
        target: join.id,
        targetHandle: null,
      },
      nodeManager,
      [],
    );

    expect(result.valid).toBe(true);
    if (!result.valid) {
      throw new Error(result.error);
    }
    const { edge } = result;
    expect(edge.type).toBe('buffer');
    expect(edge.data.input).toEqual({
      type: 'bufferKey',
      key: 'buffer_three',
    });
  });
});

describe('validate edges', () => {
  test('rejects mixed keyed and indexed buffer selections', () => {
    const source = operationNode({ type: 'buffer' }, 'source');
    const target = operationNode(
      { type: 'listen', buffers: [], next: { builtin: 'dispose' } },
      'target',
    );
    const indexed = createBufferEdge(source.id, null, target.id, null, {
      type: 'bufferSeq',
      seq: 0,
    });
    const keyed = createBufferEdge(source.id, null, target.id, null, {
      type: 'bufferKey',
      key: 'route',
    });
    const manager = new NodeManager([source, target]);
    expect(validateEdgeSimple(indexed, manager, [indexed, keyed]).valid).toBe(
      false,
    );
    expect(validateEdgeSimple(indexed, manager, [indexed]).valid).toBe(true);
  });

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
      expect(validEdges.length).toBe(1);
      expect(validEdges).toContain('default');
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

    for (const targetNode of [joinNode]) {
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

  test('"sectionBuffer" aliases an internal buffer and cannot feed a buffer consumer', () => {
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
    const buffer = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'buffer' },
      'storage',
    );
    expect(getValidEdgeTypes(sectionBuffer, null, buffer, null)).toEqual([
      'default',
    ]);

    {
      const validEdges = getValidEdgeTypes(sectionBuffer, null, node, null);
      expect(validEdges.length).toBe(0);
    }

    {
      const validEdges = getValidEdgeTypes(sectionBuffer, null, listen, null);
      expect(validEdges).toEqual([]);
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
      expect(validEdges.length).toBe(1);
      expect(validEdges).toContain('default');
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
    {
      const capacity = validateSourceOutputCapacity(nodeNode, null, edges);
      expect(capacity).toEqual({
        valid: false,
        error: 'This output can only be connected to one input',
      });
    }
    {
      const result = validateConnectionSimple(
        {
          source: nodeNode.id,
          sourceHandle: null,
          target: forkCloneNode2.id,
          targetHandle: null,
        },
        nodeManager,
        edges,
      );
      expect(result).toEqual({
        valid: false,
        error: 'This output can only be connected to one input',
      });
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

  test('"fork_result" operation only allows 1 outputs for each of the output handles', () => {
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
      // existing "ok" edge, try to add a "err" edge.
      const existingEdges = [
        createForkResultOkEdge(
          forkResultNode.id,
          HandleId.ForkResultOk,
          terminateNode.id,
          null,
        ),
      ];
      const newEdge = createForkResultErrEdge(
        forkResultNode.id,
        HandleId.ForkResultErr,
        terminateNode.id,
        null,
      );
      const result = validateEdgeSimple(newEdge, nodeManager, existingEdges);
      expect(result.valid).toBe(true);
    }

    {
      // existing "err" edge, try to add a "ok" edge.
      const existingEdges = [
        createForkResultErrEdge(
          forkResultNode.id,
          HandleId.ForkResultErr,
          terminateNode.id,
          null,
        ),
      ];
      const newEdge = createForkResultOkEdge(
        forkResultNode.id,
        HandleId.ForkResultOk,
        terminateNode.id,
        null,
      );
      const result = validateEdgeSimple(newEdge, nodeManager, existingEdges);
      expect(result.valid).toBe(true);
    }

    {
      // exisiting "ok" and "err" edge, try to add a "ok" edge.
      const existingEdges = [
        createForkResultOkEdge(
          forkResultNode.id,
          HandleId.ForkResultOk,
          terminateNode.id,
          null,
        ),
        createForkResultErrEdge(
          forkResultNode.id,
          HandleId.ForkResultErr,
          terminateNode.id,
          null,
        ),
      ];
      const newEdge = createForkResultOkEdge(
        forkResultNode.id,
        HandleId.ForkResultOk,
        terminateNode.id,
        null,
      );
      const result = validateEdgeSimple(newEdge, nodeManager, existingEdges);
      expect(result.valid).toBe(false);
    }
  });

  test('an exposed section buffer receives messages, not a buffer access edge', () => {
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
      expect(result.valid).toBe(false);
    }
  });

  test('data edges connecting to a section select an input or exposed buffer', () => {
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
    const bufferInput = createDefaultEdge(
      nodeNode.id,
      null,
      sectionNode.id,
      null,
      { type: 'sectionBuffer', inputId: 'storage' },
    );
    expect(
      validateEdgeSimple(
        bufferInput,
        new NodeManager([nodeNode, sectionNode]),
        [],
      ),
    ).toMatchObject({ valid: true });
  });
});
