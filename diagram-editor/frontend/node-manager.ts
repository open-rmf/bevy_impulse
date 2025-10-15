import { createContext, useContext } from 'react';
import type { DiagramEditorEdge } from './edges';
import {
  type DiagramEditorNode,
  isBuiltinNode,
  isOperationNode,
  TERMINATE_ID,
} from './nodes';
import type { NextOperation } from './types/api';
import { exhaustiveCheck } from './utils/exhaustive-check';
import { joinNamespaces, ROOT_NAMESPACE } from './utils/namespace';
import { isBuiltin } from './utils/operation';

export class NodeManager {
  private nodeIdMap: Map<string, DiagramEditorNode> = new Map();
  private namespacedOpIdMap: Map<string, DiagramEditorNode> = new Map();

  constructor(public nodes: DiagramEditorNode[]) {
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
      } else if (node.type === 'sectionOutput') {
        // This will conflict if there is an op in a template with the same id as an output.
        // However, this conflict exist in crossflow as well, so we assume that it will not happen.
        this.namespacedOpIdMap.set(
          joinNamespaces(ROOT_NAMESPACE, node.data.outputId),
          node,
        );
      }
    }
  }

  tryGetNode(nodeId: string): DiagramEditorNode | null {
    return this.nodeIdMap.get(nodeId) || null;
  }

  getNode(nodeId: string): DiagramEditorNode {
    const node = this.tryGetNode(nodeId);
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

  /**
   * @returns returns `null` if the next operation points to a builtin node not rendered in the editor (e.g. `dispose`, `cancel`).
   */
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
    switch (edge.data.input.type) {
      case 'bufferKey':
      case 'bufferSeq':
      case 'default': {
        const target = this.getNode(edge.target);
        if (isBuiltinNode(target)) {
          return { builtin: target.type };
        }
        if (isOperationNode(target)) {
          return target.data.opId;
        }
        if (target.type === 'sectionOutput') {
          return target.data.outputId;
        }
        throw new Error('unknown node type');
      }
      case 'sectionBuffer':
      case 'sectionInput': {
        const target = this.getNode(edge.target);
        if (target.type !== 'section') {
          throw new Error(
            'edge is connecting to a section input slot but target is not a section',
          );
        }
        return { [target.data.opId]: edge.data.input.inputId };
      }
      default: {
        exhaustiveCheck(edge.data.input);
        throw new Error('unknown edge input');
      }
    }
  }
}

const NodeManagerContext = createContext<NodeManager>(new NodeManager([]));

export const NodeManagerProvider = NodeManagerContext.Provider;

export function useNodeManager() {
  return useContext(NodeManagerContext);
}
