import type { DiagramEditorEdge } from './edges';
import {
  type DiagramEditorNode,
  isBuiltinNode,
  isOperationNode,
  TERMINATE_ID,
} from './nodes';
import type { NextOperation } from './types/api';
import {
  exhaustiveCheck,
  isBuiltin,
  joinNamespaces,
  ROOT_NAMESPACE,
} from './utils';

export class NodeManager {
  private nodeIdMap: Map<string, DiagramEditorNode> = new Map();
  private namespacedOpIdMap: Map<string, DiagramEditorNode> = new Map();

  constructor(nodes: DiagramEditorNode[]) {
    for (const node of nodes) {
      this.nodeIdMap.set(node.id, node);
      if (isOperationNode(node)) {
        const namespacedOpId = joinNamespaces(
          node.data.namespace,
          node.data.opId,
        );
        this.namespacedOpIdMap.set(namespacedOpId, node);
      } else if (isBuiltinNode(node)) {
        this.namespacedOpIdMap.set(node.id, node);
      }
    }
  }

  getNode(nodeId: string): DiagramEditorNode {
    const node = this.nodeIdMap.get(nodeId);
    if (!node) {
      throw new Error(`cannot find node "${nodeId}"`);
    }
    return node;
  }

  iterNodes(): MapIterator<DiagramEditorNode> {
    return this.nodeIdMap.values();
  }

  hasNode(nodeId: string): boolean {
    try {
      this.getNode(nodeId);
      return true;
    } catch {
      return false;
    }
  }

  getNodeFromNamespaceOpId(namespace: string, opId: string): DiagramEditorNode {
    const namespacedOpId = joinNamespaces(namespace, opId);
    const node = this.namespacedOpIdMap.get(namespacedOpId);
    if (!node) {
      throw new Error(`cannot find node for operation "${namespacedOpId}"`);
    }
    return node;
  }

  getNodeFromRootOpId(opId: string): DiagramEditorNode {
    return this.getNodeFromNamespaceOpId(ROOT_NAMESPACE, opId);
  }

  getNodeFromNextOp(
    namespace: string,
    nextOp: NextOperation,
  ): DiagramEditorNode | null {
    if (isBuiltin(nextOp)) {
      switch (nextOp.builtin) {
        case 'dispose':
        case 'cancel':
          return null;
        case 'terminate':
          return this.getNode(joinNamespaces(namespace, TERMINATE_ID));
      }
    }

    const opId = (() => {
      if (typeof nextOp === 'object') {
        return Object.keys(nextOp)[0];
      }
      return nextOp;
    })();

    const namespacedOpId = joinNamespaces(namespace, opId);
    const node = this.namespacedOpIdMap.get(namespacedOpId);
    if (!node) {
      throw new Error(
        `cannot find operation ${joinNamespaces(namespace, opId)}`,
      );
    }
    return node;
  }

  getTargetNextOp(edge: DiagramEditorEdge): NextOperation {
    // TODO: Validate that the edge does not traverse namespaces
    switch (edge.type) {
      case 'bufferKey':
      case 'bufferSeq':
      case 'default':
      case 'forkResultOk':
      case 'forkResultErr':
      case 'splitKey':
      case 'splitSeq':
      case 'splitRemaining':
      case 'streamOut':
      case 'unzip': {
        const target = this.getNode(edge.target);
        if (isBuiltinNode(target)) {
          return { builtin: target.type };
        }
        if (isOperationNode(target)) {
          return target.data.opId;
        }
        throw new Error('unknown node type');
      }
      // TODO: For section edges, return a `{ [target.data.opId]: edge.data.input }`
      default: {
        exhaustiveCheck(edge);
        throw new Error('unknown edge');
      }
    }
  }
}
