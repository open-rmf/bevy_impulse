import {
  type DiagramEditorEdge,
  type DiagramEditorNode,
  START_ID,
  TERMINATE_ID,
} from '../nodes';
import type { DiagramOperation } from '../types/diagram';
import { addConnection } from './connection';

describe('addConnection', () => {
  let fromNode: DiagramEditorNode;
  let edge: DiagramEditorEdge;

  beforeEach(() => {
    // Reset nodes and edges for each test
    fromNode = {
      id: 'testNode',
      type: 'inputOutput',
      position: { x: 0, y: 0 },
      data: {
        opId: 'testOp',
        type: 'node',
        builder: 'testBuilder',
        next: '',
      },
    };
    edge = {
      id: 'testEdge',
      source: 'testNode',
      target: 'node2',
      data: undefined,
    };
  });

  it('should throw an error if source node is "start"', () => {
    fromNode.id = START_ID;
    expect(() => addConnection(fromNode, edge)).toThrow(
      'source node cannot be "start"',
    );
  });

  it('should throw an error if source node is "terminate"', () => {
    fromNode.id = TERMINATE_ID;
    expect(() => addConnection(fromNode, edge)).toThrow(
      'source node cannot be "terminate"',
    );
  });

  it('should throw an error for "buffer" node type', () => {
    fromNode.data = { opId: 'testOp', type: 'buffer' };
    expect(() => addConnection(fromNode, edge)).toThrow(
      'buffer operations cannot have connections',
    );
  });

  // Test cases for 'node', 'join', 'serialized_join', 'transform', 'buffer_access', 'listen'
  for (const type of [
    'node',
    'join',
    'serialized_join',
    'transform',
    'buffer_access',
    'listen',
  ] as DiagramOperation['type'][]) {
    it(`should set next for "${type}" node type`, () => {
      // biome-ignore lint/suspicious/noExplicitAny: using any so we don't have to fully define the operations
      fromNode.data = { opId: 'testOp', type, next: '' } as any;
      addConnection(fromNode, edge);
      expect(fromNode.data.next).toBe('node2');
    });

    it(`should be idempotent for "${type}" node type`, () => {
      // biome-ignore lint/suspicious/noExplicitAny: using any so we don't have to fully define the operations
      fromNode.data = { type: type as any, next: 'node2' } as any;
      addConnection(fromNode, edge);
      expect(fromNode.data.next).toBe('node2');
    });
  }

  describe('fork_clone', () => {
    let forkCloneData: Extract<
      DiagramEditorNode['data'],
      { type: 'fork_clone' }
    >;

    beforeEach(() => {
      forkCloneData = { opId: 'testOp', type: 'fork_clone', next: [] };
      fromNode.data = forkCloneData;
    });

    it('should add target to next array if not present', () => {
      addConnection(fromNode, edge);
      expect(forkCloneData.next).toEqual(['node2']);
    });

    it('should not add target if already present', () => {
      fromNode.data.next = ['node2'];
      addConnection(fromNode, edge);
      expect(forkCloneData.next).toEqual(['node2']);
    });

    it('should add a different target', () => {
      fromNode.data.next = ['node3'];
      addConnection(fromNode, edge);
      expect(forkCloneData.next).toEqual(['node3', 'node2']);
    });
  });

  describe('unzip', () => {
    let unzipData: Extract<DiagramEditorNode['data'], { type: 'unzip' }>;

    beforeEach(() => {
      unzipData = { opId: 'testOp', type: 'unzip', next: [] };
      fromNode.data = unzipData;
    });

    it('should set target at specified sequence in next array for "unzip" edge data type', () => {
      edge.data = { type: 'unzip', seq: 1 };
      addConnection(fromNode, edge);
      expect(unzipData.next[1]).toBe('node2');
      expect(unzipData.next.length).toBe(2); // JS allows sparse arrays
    });

    it('should overwrite target at specified sequence', () => {
      unzipData.next[1] = 'oldNode';
      edge.data = { type: 'unzip', seq: 1 };
      addConnection(fromNode, edge);
      expect(unzipData.next[1]).toBe('node2');
    });

    it('should do nothing if edge data type is not "unzip"', () => {
      edge.data = { type: 'ok' };
      const originalNext = [...unzipData.next];
      addConnection(fromNode, edge);
      expect(unzipData.next).toEqual(originalNext);
    });

    it('should do nothing if edge data is undefined', () => {
      edge.data = undefined;
      const originalNext = [...unzipData.next];
      addConnection(fromNode, edge);
      expect(unzipData.next).toEqual(originalNext);
    });
  });

  describe('fork_result', () => {
    let forkResultData: Extract<
      DiagramEditorNode['data'],
      { type: 'fork_result' }
    >;

    beforeEach(() => {
      forkResultData = { opId: 'testOp', type: 'fork_result', ok: '', err: '' };
      fromNode.data = forkResultData;
    });

    it('should set "ok" target for "ok" edge data type', () => {
      edge.data = { type: 'ok' };
      addConnection(fromNode, edge);
      expect(forkResultData.ok).toBe('node2');
      expect(forkResultData.err).toBe('');
    });

    it('should set "err" target for "err" edge data type', () => {
      edge.data = { type: 'err' };
      addConnection(fromNode, edge);
      expect(forkResultData.err).toBe('node2');
      expect(forkResultData.ok).toBe('');
    });

    it('should do nothing for other edge data types', () => {
      edge.data = { type: 'unzip', seq: 0 };
      addConnection(fromNode, edge);
      expect(forkResultData.ok).toBe('');
      expect(forkResultData.err).toBe('');
    });
  });

  describe('split', () => {
    let splitData: Extract<DiagramEditorNode['data'], { type: 'split' }>;

    beforeEach(() => {
      splitData = {
        opId: 'testOp',
        type: 'split',
      };
      fromNode.data = splitData;
    });

    describe('splitKey', () => {
      it('should initialize keyed and set target for "splitKey" edge data type', () => {
        edge.data = { type: 'splitKey', key: 'myKey' };
        addConnection(fromNode, edge);
        expect(splitData.keyed).toEqual({ myKey: 'node2' });
        expect(splitData.sequential).toBeUndefined();
        expect(splitData.remaining).toBeUndefined();
      });

      it('should add to existing keyed object for "splitKey"', () => {
        fromNode.data.keyed = { existingKey: 'node0' };
        edge.data = { type: 'splitKey', key: 'myKey' };
        addConnection(fromNode, edge);
        expect(splitData.keyed).toEqual({
          existingKey: 'node0',
          myKey: 'node2',
        });
      });

      it('should overwrite existing key for "splitKey"', () => {
        fromNode.data.keyed = { myKey: 'oldNode' };
        edge.data = { type: 'splitKey', key: 'myKey' };
        addConnection(fromNode, edge);
        expect(splitData.keyed).toEqual({ myKey: 'node2' });
      });
    });

    describe('splitSequential', () => {
      it('should initialize sequential and set target for "splitSequential" edge data type', () => {
        edge.data = { type: 'splitSequential', seq: 0 };
        addConnection(fromNode, edge);
        expect(splitData.sequential?.[0]).toBe('node2');
        expect(splitData.keyed).toBeUndefined();
        expect(splitData.remaining).toBeUndefined();
      });

      it('should add to existing sequential array for "splitSequential"', () => {
        fromNode.data.sequential = ['node0'];
        edge.data = { type: 'splitSequential', seq: 1 };
        addConnection(fromNode, edge);
        expect(splitData.sequential?.[0]).toBe('node0');
        expect(splitData.sequential?.[1]).toBe('node2');
      });

      it('should overwrite existing sequence for "splitSequential"', () => {
        fromNode.data.sequential = ['node0', 'oldNode'];
        edge.data = { type: 'splitSequential', seq: 1 };
        addConnection(fromNode, edge);
        expect(splitData.sequential?.[1]).toBe('node2');
      });

      it('should handle non-sequential array indices for "splitSequential"', () => {
        edge.data = { type: 'splitSequential', seq: 2 };
        addConnection(fromNode, edge);
        expect(splitData.sequential?.[2]).toBe('node2');
        expect(splitData.sequential?.length).toBe(3); // JS allows sparse arrays
      });
    });

    describe('splitRemaining', () => {
      it('should set remaining target for "splitRemaining" edge data type', () => {
        edge.data = { type: 'splitRemaining' };
        addConnection(fromNode, edge);
        expect(splitData.remaining).toBe('node2');
        expect(splitData.keyed).toBeUndefined();
        expect(splitData.sequential).toBeUndefined();
      });

      it('should overwrite existing remaining target', () => {
        fromNode.data.remaining = 'oldNode';
        edge.data = { type: 'splitRemaining' };
        addConnection(fromNode, edge);
        expect(splitData.remaining).toBe('node2');
      });
    });

    it('should do nothing for other edge data types', () => {
      edge.data = { type: 'ok' };
      addConnection(fromNode, edge);
      expect(splitData.keyed).toBeUndefined();
      expect(splitData.sequential).toBeUndefined();
      expect(splitData.remaining).toBeUndefined();
    });

    it('should do nothing if edge data is undefined', () => {
      edge.data = undefined;
      addConnection(fromNode, edge);
      expect(splitData.keyed).toBeUndefined();
      expect(splitData.sequential).toBeUndefined();
      expect(splitData.remaining).toBeUndefined();
    });
  });
});
