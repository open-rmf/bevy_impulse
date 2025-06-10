import { type DiagramEditorEdge, EdgeType, TERMINATE_ID } from '../nodes';
import type { Diagram, DiagramOperation } from '../types/diagram';
import { syncEdge } from './connection';

describe('syncEdge', () => {
  let diagram: Diagram;
  let testOp1: DiagramOperation;
  let edge: DiagramEditorEdge;

  beforeEach(() => {
    testOp1 = {
      type: 'node',
      builder: 'testBuilder',
      next: { builtin: 'dispose' },
    };
    diagram = {
      start: 'testOp1',
      version: '0.1.0',
      ops: {
        testOp1,
        testOp2: {
          type: 'node',
          builder: 'testBuilder',
          next: { builtin: 'terminate' },
        },
      },
    };
    edge = {
      id: 'testEdge',
      source: 'testOp1',
      target: 'testOp2',
      data: {
        type: EdgeType.Basic,
      },
    };
  });

  it('should throw an error if source is "terminate"', () => {
    edge.source = TERMINATE_ID;
    expect(() => syncEdge(diagram, edge)).toThrow();
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
      testOp1.type = type;
      syncEdge(diagram, edge);
      expect(testOp1.next).toBe('testOp2');
      syncEdge(diagram, edge);
      expect(testOp1.next).toBe('testOp2');
    });
  }

  describe('fork_clone', () => {
    beforeEach(() => {
      testOp1 = { type: 'fork_clone', next: [] };
      diagram.ops.testOp1 = testOp1;
    });

    it('should add target to next array if not present', () => {
      syncEdge(diagram, edge);
      expect(testOp1.next).toEqual(['testOp2']);
    });

    it('should not add target if already present', () => {
      testOp1.next = ['testOp2'];
      syncEdge(diagram, edge);
      expect(testOp1.next).toEqual(['testOp2']);
    });

    it('should add a different target', () => {
      testOp1.next = ['node3'];
      syncEdge(diagram, edge);
      expect(testOp1.next).toEqual(['node3', 'testOp2']);
    });
  });

  describe('unzip', () => {
    let testOp1: Extract<DiagramOperation, { type: 'unzip' }>;

    beforeEach(() => {
      testOp1 = { type: 'unzip', next: [] };
      diagram.ops.testOp1 = testOp1;
    });

    it('should set target at specified sequence in next array for "unzip" edge data type', () => {
      edge.data = { type: EdgeType.Unzip, seq: 1 };
      syncEdge(diagram, edge);
      expect(testOp1.next[1]).toBe('testOp2');
      expect(testOp1.next.length).toBe(2); // JS allows sparse arrays
    });

    it('should overwrite target at specified sequence', () => {
      testOp1.next[1] = 'oldNode';
      edge.data = { type: EdgeType.Unzip, seq: 1 };
      syncEdge(diagram, edge);
      expect(testOp1.next[1]).toBe('testOp2');
    });

    it('should do nothing if edge data type is not "unzip"', () => {
      edge.data = { type: EdgeType.Basic };
      const originalNext = [...testOp1.next];
      syncEdge(diagram, edge);
      expect(testOp1.next).toEqual(originalNext);
    });
  });

  describe('fork_result', () => {
    let testOp1: Extract<DiagramOperation, { type: 'fork_result' }>;

    beforeEach(() => {
      testOp1 = {
        type: 'fork_result',
        ok: { builtin: 'dispose' },
        err: { builtin: 'dispose' },
      };
      diagram.ops.testOp1 = testOp1;
    });

    it('should set "ok" target for "ok" edge data type', () => {
      edge.data = { type: EdgeType.ForkResultOk };
      syncEdge(diagram, edge);
      expect(testOp1.ok).toBe('testOp2');
      expect(testOp1.err).toEqual({ builtin: 'dispose' });
    });

    it('should set "err" target for "err" edge data type', () => {
      edge.data = { type: EdgeType.ForkResultErr };
      syncEdge(diagram, edge);
      expect(testOp1.err).toBe('testOp2');
      expect(testOp1.ok).toEqual({ builtin: 'dispose' });
    });

    it('should throw error for other edge data types', () => {
      edge.data = { type: EdgeType.Basic };
      expect(() => syncEdge(diagram, edge)).toThrow();
    });
  });

  describe('split', () => {
    let testOp1: Extract<DiagramOperation, { type: 'split' }>;

    beforeEach(() => {
      testOp1 = {
        type: 'split',
      };
      diagram.ops.testOp1 = testOp1;
    });

    describe('splitKey', () => {
      it('should initialize keyed and set target for "splitKey" edge data type', () => {
        edge.data = { type: EdgeType.SplitKey, key: 'myKey' };
        syncEdge(diagram, edge);
        expect(testOp1.keyed).toEqual({ myKey: 'testOp2' });
        expect(testOp1.sequential).toBeUndefined();
        expect(testOp1.remaining).toBeUndefined();
      });

      it('should add to existing keyed object for "splitKey"', () => {
        testOp1.keyed = { existingKey: 'node0' };
        edge.data = { type: EdgeType.SplitKey, key: 'myKey' };
        syncEdge(diagram, edge);
        expect(testOp1.keyed).toEqual({
          existingKey: 'node0',
          myKey: 'testOp2',
        });
      });

      it('should overwrite existing key for "splitKey"', () => {
        testOp1.keyed = { myKey: 'oldNode' };
        edge.data = { type: EdgeType.SplitKey, key: 'myKey' };
        syncEdge(diagram, edge);
        expect(testOp1.keyed).toEqual({ myKey: 'testOp2' });
      });
    });

    describe('splitSequential', () => {
      it('should initialize sequential and set target for "splitSequential" edge data type', () => {
        edge.data = { type: EdgeType.SplitSequential, seq: 0 };
        syncEdge(diagram, edge);
        expect(testOp1.sequential?.[0]).toBe('testOp2');
        expect(testOp1.keyed).toBeUndefined();
        expect(testOp1.remaining).toBeUndefined();
      });

      it('should add to existing sequential array for "splitSequential"', () => {
        testOp1.sequential = ['node0'];
        edge.data = { type: EdgeType.SplitSequential, seq: 1 };
        syncEdge(diagram, edge);
        expect(testOp1.sequential?.[0]).toBe('node0');
        expect(testOp1.sequential?.[1]).toBe('testOp2');
      });

      it('should overwrite existing sequence for "splitSequential"', () => {
        testOp1.sequential = ['node0', 'oldNode'];
        edge.data = { type: EdgeType.SplitSequential, seq: 1 };
        syncEdge(diagram, edge);
        expect(testOp1.sequential?.[1]).toBe('testOp2');
      });

      it('should handle non-sequential array indices for "splitSequential"', () => {
        edge.data = { type: EdgeType.SplitSequential, seq: 2 };
        syncEdge(diagram, edge);
        expect(testOp1.sequential?.[2]).toBe('testOp2');
        expect(testOp1.sequential?.length).toBe(3); // JS allows sparse arrays
      });
    });

    describe(EdgeType.SplitRemaining, () => {
      it('should set remaining target for "splitRemaining" edge data type', () => {
        edge.data = { type: EdgeType.SplitRemaining };
        syncEdge(diagram, edge);
        expect(testOp1.remaining).toBe('testOp2');
        expect(testOp1.keyed).toBeUndefined();
        expect(testOp1.sequential).toBeUndefined();
      });

      it('should overwrite existing remaining target', () => {
        testOp1.remaining = 'oldNode';
        edge.data = { type: EdgeType.SplitRemaining };
        syncEdge(diagram, edge);
        expect(testOp1.remaining).toBe('testOp2');
      });
    });

    it('should throw for other edge data types', () => {
      edge.data = { type: EdgeType.Basic };
      expect(() => syncEdge(diagram, edge)).toThrow();
    });
  });
});
