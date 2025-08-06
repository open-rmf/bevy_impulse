import type {
  BuiltinNode,
  BuiltinNodeTypes,
  DiagramEditorNode,
  OperationNode,
  OperationNodeTypes,
  SectionInterfaceNode,
} from '.';

export function isOperationData(
  data: DiagramEditorNode['data'],
): data is OperationNode['data'] {
  return 'type' in data;
}

const OPERATION_NODE_TYPES = Object.keys({
  buffer: null,
  buffer_access: null,
  fork_clone: null,
  fork_result: null,
  join: null,
  listen: null,
  node: null,
  scope: null,
  section: null,
  serialized_join: null,
  split: null,
  stream_out: null,
  transform: null,
  unzip: null,
} satisfies Record<OperationNodeTypes, null>);

export function isOperationNode(
  node: DiagramEditorNode,
): node is OperationNode {
  return OPERATION_NODE_TYPES.includes(node.type);
}

const BUILTIN_NODE_TYPES = Object.keys({
  start: null,
  terminate: null,
} satisfies Record<BuiltinNodeTypes, null>);

export function isBuiltinNode(node: DiagramEditorNode): node is BuiltinNode {
  return BUILTIN_NODE_TYPES.includes(node.type);
}

export function isScopeNode(
  node: DiagramEditorNode,
): node is OperationNode<'scope'> {
  return node.type === 'scope';
}

export function isSectionInterfaceNode(
  node: DiagramEditorNode,
): node is SectionInterfaceNode {
  return (
    node.type === 'sectionInput' ||
    node.type === 'sectionOutput' ||
    node.type === 'sectionBuffer'
  );
}

export function isSectionNode(
  node: DiagramEditorNode,
): node is OperationNode<'section'> {
  return node.type === 'section';
}
