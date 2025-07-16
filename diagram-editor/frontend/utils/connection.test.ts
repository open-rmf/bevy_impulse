import { v4 as uuidv4 } from 'uuid';
import { NodeManager } from '../node-manager';
import { TERMINATE_ID } from '../nodes';
import type {
  Diagram,
  DiagramEditorEdge,
  DiagramEditorNode,
  DiagramOperation,
  OperationNode,
} from '../types';
import { loadDiagram } from './load-diagram';
import { joinNamespaces, ROOT_NAMESPACE } from './namespace';

function createNode(op: DiagramOperation): OperationNode {
  return {
    id: uuidv4(),
    position: { x: 0, y: 0 },
    type: op.type,
    data: {
      op,
      opId: uuidv4(),
      namespace: ROOT_NAMESPACE,
    },
  };
}

describe('syncEdge', () => {
  let nodes: DiagramEditorNode[];

  beforeEach(() => {
    const emptyDiagram = {
      start: { builtin: 'dispose' },
      version: '0.1.0',
      ops: {},
    } satisfies Diagram;
    nodes = loadDiagram(emptyDiagram).nodes;
  });

  // Test cases for 'node', 'join', 'serialized_join', 'transform', 'buffer_access', 'listen'
  for (const op of [
    {
      type: 'node',
      builder: '',
      next: { builtin: 'terminate' },
    },
    {
      type: 'join',
      buffers: [],
      next: { builtin: 'terminate' },
    },
    {
      type: 'serialized_join',
      buffers: [],
      next: { builtin: 'terminate' },
    },
    {
      type: 'transform',
      cel: '',
      next: { builtin: 'terminate' },
    },
    {
      type: 'buffer_access',
      buffers: [],
      next: { builtin: 'terminate' },
    },
    {
      type: 'listen',
      buffers: [],
      next: { builtin: 'terminate' },
    },
  ] satisfies DiagramOperation[]) {
    it(`should set next for "${op.type}"`, () => {
      const node = createNode(op);
      nodes.push(node);
      const edge: DiagramEditorEdge = {
        id: uuidv4(),
        type: 'default',
        source: node.id,
        target: joinNamespaces(ROOT_NAMESPACE, TERMINATE_ID),
        data: {},
      };
      const nodeManager = new NodeManager(nodes);
      nodeManager.syncEdge(edge);
      expect(node.data.op.next).toEqual({ builtin: 'terminate' });
    });
  }

  describe('fork_clone', () => {
    let forkCloneNode: DiagramEditorNode;
    let forkCloneOp: DiagramOperation & { type: 'fork_clone' };
    let nodeManager: NodeManager;

    beforeEach(() => {
      forkCloneOp = {
        type: 'fork_clone',
        next: [],
      };
      forkCloneNode = createNode(forkCloneOp);
      nodes.push(forkCloneNode);

      nodeManager = new NodeManager(nodes);
    });

    it('should add target to next array if not present', () => {
      nodeManager.syncEdge({
        id: uuidv4(),
        type: 'default',
        source: forkCloneNode.id,
        target: joinNamespaces(ROOT_NAMESPACE, TERMINATE_ID),
        data: {},
      });
      expect(forkCloneOp.next).toEqual([{ builtin: 'terminate' }]);
    });

    it('should not add target if already present', () => {
      forkCloneOp.next = [{ builtin: 'terminate' }];
      nodeManager.syncEdge({
        id: uuidv4(),
        type: 'default',
        source: forkCloneNode.id,
        target: joinNamespaces(ROOT_NAMESPACE, TERMINATE_ID),
        data: {},
      });
      expect(forkCloneOp.next).toEqual([{ builtin: 'terminate' }]);
    });

    it('should add a different target', () => {
      forkCloneOp.next = [{ builtin: 'dispose' }];
      nodeManager.syncEdge({
        id: uuidv4(),
        type: 'default',
        source: forkCloneNode.id,
        target: joinNamespaces(ROOT_NAMESPACE, TERMINATE_ID),
        data: {},
      });
      expect(forkCloneOp.next).toEqual([
        { builtin: 'dispose' },
        { builtin: 'terminate' },
      ]);
    });

    it('should throw for other edge data types', () => {
      expect(() =>
        nodeManager.syncEdge({
          id: uuidv4(),
          type: 'forkResultOk',
          source: forkCloneNode.id,
          target: joinNamespaces(ROOT_NAMESPACE, TERMINATE_ID),
          data: {},
        }),
      ).toThrow();
    });
  });

  describe('unzip', () => {
    let unzipNode: DiagramEditorNode;
    let unzipOp: DiagramOperation & { type: 'unzip' };
    let nodeManager: NodeManager;

    beforeEach(() => {
      unzipOp = {
        type: 'unzip',
        next: [],
      };
      unzipNode = createNode(unzipOp);
      nodes.push(unzipNode);

      nodeManager = new NodeManager(nodes);
    });

    it('should set target at specified sequence in next array for "unzip" edge data type', () => {
      nodeManager.syncEdge({
        id: uuidv4(),
        type: 'unzip',
        source: unzipNode.id,
        target: joinNamespaces(ROOT_NAMESPACE, TERMINATE_ID),
        data: { seq: 1 },
      });
      expect(unzipOp.next[1]).toEqual({ builtin: 'terminate' });
    });

    it('should overwrite target at specified sequence', () => {
      unzipOp.next[1] = 'oldNode';
      nodeManager.syncEdge({
        id: uuidv4(),
        type: 'unzip',
        source: unzipNode.id,
        target: joinNamespaces(ROOT_NAMESPACE, TERMINATE_ID),
        data: { seq: 1 },
      });
      expect(unzipOp.next[1]).toEqual({ builtin: 'terminate' });
    });

    it('should throw for other edge data types', () => {
      expect(() =>
        nodeManager.syncEdge({
          id: uuidv4(),
          type: 'forkResultOk',
          source: unzipNode.id,
          target: joinNamespaces(ROOT_NAMESPACE, TERMINATE_ID),
          data: {},
        }),
      ).toThrow();
    });
  });

  describe('fork_result', () => {
    let forkResultNode: DiagramEditorNode;
    let forkResultOp: DiagramOperation & { type: 'fork_result' };
    let nodeManager: NodeManager;

    beforeEach(() => {
      forkResultOp = {
        type: 'fork_result',
        ok: { builtin: 'dispose' },
        err: { builtin: 'dispose' },
      };
      forkResultNode = createNode(forkResultOp);
      nodes.push(forkResultNode);

      nodeManager = new NodeManager(nodes);
    });

    it('should set "ok" target for "ok" edge data type', () => {
      nodeManager.syncEdge({
        id: uuidv4(),
        type: 'forkResultOk',
        source: forkResultNode.id,
        target: joinNamespaces(ROOT_NAMESPACE, TERMINATE_ID),
        data: {},
      });
      expect(forkResultOp.ok).toEqual({ builtin: 'terminate' });
      expect(forkResultOp.err).toEqual({ builtin: 'dispose' });
    });

    it('should set "err" target for "err" edge data type', () => {
      nodeManager.syncEdge({
        id: uuidv4(),
        type: 'forkResultErr',
        source: forkResultNode.id,
        target: joinNamespaces(ROOT_NAMESPACE, TERMINATE_ID),
        data: {},
      });
      expect(forkResultOp.ok).toEqual({ builtin: 'dispose' });
      expect(forkResultOp.err).toEqual({ builtin: 'terminate' });
    });

    it('should throw error for other edge data types', () => {
      expect(() =>
        nodeManager.syncEdge({
          id: uuidv4(),
          type: 'default',
          source: forkResultNode.id,
          target: joinNamespaces(ROOT_NAMESPACE, TERMINATE_ID),
          data: {},
        }),
      ).toThrow();
    });
  });

  describe('split', () => {
    let splitNode: DiagramEditorNode;
    let splitOp: DiagramOperation & { type: 'split' };
    let nodeManager: NodeManager;

    beforeEach(() => {
      splitOp = {
        type: 'split',
      };
      splitNode = createNode(splitOp);
      nodes.push(splitNode);

      nodeManager = new NodeManager(nodes);
    });

    describe('splitKey', () => {
      it('should initialize keyed and set target for "splitKey" edge data type', () => {
        nodeManager.syncEdge({
          id: uuidv4(),
          type: 'splitKey',
          source: splitNode.id,
          target: joinNamespaces(ROOT_NAMESPACE, TERMINATE_ID),
          data: { key: 'testKey' },
        });
        expect(splitOp.keyed).toEqual({ testKey: { builtin: 'terminate' } });
        expect(splitOp.sequential).toBeUndefined();
        expect(splitOp.remaining).toBeUndefined();
      });

      it('should add to existing keyed object for "splitKey"', () => {
        splitOp.keyed = { existingKey: { builtin: 'terminate' } };
        nodeManager.syncEdge({
          id: uuidv4(),
          type: 'splitKey',
          source: splitNode.id,
          target: joinNamespaces(ROOT_NAMESPACE, TERMINATE_ID),
          data: { key: 'testKey' },
        });
        expect(splitOp.keyed).toEqual({
          existingKey: { builtin: 'terminate' },
          testKey: { builtin: 'terminate' },
        });
        expect(splitOp.sequential).toBeUndefined();
        expect(splitOp.remaining).toBeUndefined();
      });

      it('should overwrite existing key for "splitKey"', () => {
        splitOp.keyed = { testKey: { builtin: 'dispose' } };
        nodeManager.syncEdge({
          id: uuidv4(),
          type: 'splitKey',
          source: splitNode.id,
          target: joinNamespaces(ROOT_NAMESPACE, TERMINATE_ID),
          data: { key: 'testKey' },
        });
        expect(splitOp.keyed).toEqual({
          testKey: { builtin: 'terminate' },
        });
        expect(splitOp.sequential).toBeUndefined();
        expect(splitOp.remaining).toBeUndefined();
      });
    });

    describe('splitSeq', () => {
      it('should initialize sequential and set target for "splitSequential" edge data type', () => {
        nodeManager.syncEdge({
          id: uuidv4(),
          type: 'splitSeq',
          source: splitNode.id,
          target: joinNamespaces(ROOT_NAMESPACE, TERMINATE_ID),
          data: { seq: 0 },
        });
        expect(splitOp.sequential?.[0]).toEqual({ builtin: 'terminate' });
        expect(splitOp.keyed).toBeUndefined();
        expect(splitOp.remaining).toBeUndefined();
      });

      it('should add to existing sequential array for "splitSequential"', () => {
        splitOp.sequential = [{ builtin: 'terminate' }];
        nodeManager.syncEdge({
          id: uuidv4(),
          type: 'splitSeq',
          source: splitNode.id,
          target: joinNamespaces(ROOT_NAMESPACE, TERMINATE_ID),
          data: { seq: 1 },
        });
        expect(splitOp.sequential?.length).toBe(2);
        expect(splitOp.sequential?.[0]).toEqual({ builtin: 'terminate' });
        expect(splitOp.sequential?.[1]).toEqual({ builtin: 'terminate' });
        expect(splitOp.keyed).toBeUndefined();
        expect(splitOp.remaining).toBeUndefined();
      });

      it('should overwrite existing sequence for "splitSequential"', () => {
        splitOp.sequential = [{ builtin: 'dispose' }];
        nodeManager.syncEdge({
          id: uuidv4(),
          type: 'splitSeq',
          source: splitNode.id,
          target: joinNamespaces(ROOT_NAMESPACE, TERMINATE_ID),
          data: { seq: 0 },
        });
        expect(splitOp.sequential?.[0]).toEqual({ builtin: 'terminate' });
        expect(splitOp.keyed).toBeUndefined();
        expect(splitOp.remaining).toBeUndefined();
      });

      it('should handle non-sequential array indices for "splitSequential"', () => {
        splitOp.sequential = [];
        nodeManager.syncEdge({
          id: uuidv4(),
          type: 'splitSeq',
          source: splitNode.id,
          target: joinNamespaces(ROOT_NAMESPACE, TERMINATE_ID),
          data: { seq: 2 },
        });
        expect(splitOp.sequential?.[2]).toEqual({ builtin: 'terminate' });
        expect(splitOp.keyed).toBeUndefined();
        expect(splitOp.remaining).toBeUndefined();
      });
    });

    describe('splitRemaining', () => {
      it('should set remaining target for "splitRemaining" edge data type', () => {
        nodeManager.syncEdge({
          id: uuidv4(),
          type: 'splitRemaining',
          source: splitNode.id,
          target: joinNamespaces(ROOT_NAMESPACE, TERMINATE_ID),
          data: {},
        });
        expect(splitOp.remaining).toEqual({ builtin: 'terminate' });
        expect(splitOp.sequential).toBeUndefined();
        expect(splitOp.keyed).toBeUndefined();
      });

      it('should overwrite existing remaining target', () => {
        splitOp.remaining = { builtin: 'dispose' };
        nodeManager.syncEdge({
          id: uuidv4(),
          type: 'splitRemaining',
          source: splitNode.id,
          target: joinNamespaces(ROOT_NAMESPACE, TERMINATE_ID),
          data: {},
        });
        expect(splitOp.remaining).toEqual({ builtin: 'terminate' });
        expect(splitOp.sequential).toBeUndefined();
        expect(splitOp.keyed).toBeUndefined();
      });
    });

    it('should throw for other edge data types', () => {
      expect(() =>
        nodeManager.syncEdge({
          id: uuidv4(),
          type: 'default',
          source: splitNode.id,
          target: joinNamespaces(ROOT_NAMESPACE, TERMINATE_ID),
          data: {},
        }),
      ).toThrow();
    });
  });
});
