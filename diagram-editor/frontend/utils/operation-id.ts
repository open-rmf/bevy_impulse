import { applyNodeChanges, type NodeChange } from '@xyflow/react';
import {
  type DiagramEditorNode,
  isBuiltinNode,
  isOperationNode,
  type OperationNode,
} from '../nodes';
import type { DiagramOperation } from '../types/api';
import { joinNamespaces } from './namespace';

function operationPrefix(op: DiagramOperation): string {
  switch (op.type) {
    case 'fork_clone':
      return 'clone';
    case 'fork_result':
      return 'result';
    case 'node':
      return ['node', op.builder].filter(Boolean).join('_');
    case 'script':
      return ['script', op.run].filter(Boolean).join('_');
    default:
      return op.type;
  }
}

export function generateOperationId(
  op: DiagramOperation,
  existing: Iterable<string> = [],
  suffix?: string,
): string {
  const prefix = operationPrefix(op).replace(/[^a-zA-Z0-9_]+/g, '_');
  const used = new Set(existing);
  let id: string;
  do {
    suffix ??= Array.from(
      crypto.getRandomValues(new Uint8Array(6)),
      (value) => 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'[value % 36],
    ).join('');
    id = `${prefix}_${suffix}`;
    suffix = undefined;
  } while (used.has(id));
  return id;
}

/** Update only IDs owned by the editor; the graph's internal node IDs stay stable. */
export function updateOperationIds(
  changes: NodeChange<DiagramEditorNode>[],
  nodes: DiagramEditorNode[],
): NodeChange<DiagramEditorNode>[] {
  let operations = nodes.filter(isOperationNode);
  const result = changes.map((change) => {
    if (change.type !== 'add' && change.type !== 'replace') return change;
    let item = change.item;
    if (!isOperationNode(item)) return change;
    operations = operations.filter((node) => node.id !== item.id);
    if (item.data.op.extensions?.['crossflow-auto-id'] === true) {
      const existing = operations
        .filter((node) => node.parentId === item.parentId)
        .map((node) => node.data.opId);
      const opId = generateOperationId(
        item.data.op,
        existing,
        item.data.opId.slice(-6) || undefined,
      );
      if (opId !== item.data.opId)
        item = { ...item, data: { ...item.data, opId } };
    }
    operations.push(item);
    return { ...change, item };
  });
  // A scope's operation ID is part of every descendant's namespace.
  if (
    result.some(
      (change) =>
        (change.type === 'add' || change.type === 'replace') &&
        change.item.type === 'scope',
    )
  ) {
    const graph = applyNodeChanges(result, nodes);
    const byId = new Map(graph.map((node) => [node.id, node]));
    function namespace(node: DiagramEditorNode): string {
      if (!node.parentId) return '';
      const parent = byId.get(node.parentId) as OperationNode<'scope'>;
      return joinNamespaces(namespace(parent), parent.data.opId);
    }
    for (const node of graph) {
      if (!isOperationNode(node) && !isBuiltinNode(node)) continue;
      const nextNamespace = namespace(node);
      if (node.data.namespace === nextNamespace) continue;
      const item = {
        ...node,
        data: { ...node.data, namespace: nextNamespace },
      } as typeof node;
      const change = result.find(
        (change) =>
          (change.type === 'add' || change.type === 'replace') &&
          change.item.id === node.id,
      );
      if (change && (change.type === 'add' || change.type === 'replace'))
        change.item = item;
      else result.push({ type: 'replace', id: node.id, item });
    }
  }
  return result;
}
