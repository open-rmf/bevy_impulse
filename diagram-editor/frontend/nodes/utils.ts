import type { BuiltinNode, DiagramEditorNode, OperationNode } from '.';

export function isOperationData(
  data: DiagramEditorNode['data'],
): data is OperationNode['data'] {
  return 'type' in data;
}

export function isOperationNode(
  node: DiagramEditorNode,
): node is OperationNode {
  return node.type ? !['start', 'terminate'].includes(node.type) : false;
}

export function isBuiltinNode(node: DiagramEditorNode): node is BuiltinNode {
  return node.type ? ['start', 'terminate'].includes(node.type) : false;
}
