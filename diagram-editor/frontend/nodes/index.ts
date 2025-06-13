import type { DiagramOperation } from '../types/diagram';
import { InputOutputNode } from './input-output-node';
import { StartNode } from './start-node';
import { TerminateNode } from './terminate-node';
import type { DiagramEditorNode, NodeTypes, OperationNode } from './types';

export * from './types';

export const START_ID = 'builtin:start';
export const TERMINATE_ID = 'builtin:terminate';

export const NODE_TYPES = {
  start: StartNode,
  terminate: TerminateNode,
  node: InputOutputNode,
  section: InputOutputNode,
  fork_clone: InputOutputNode,
  unzip: InputOutputNode,
  fork_result: InputOutputNode,
  split: InputOutputNode,
  join: InputOutputNode,
  serialized_join: InputOutputNode,
  transform: InputOutputNode,
  buffer: InputOutputNode,
  buffer_access: InputOutputNode,
  listen: InputOutputNode,
} satisfies Record<NodeTypes, unknown>;

export function isOperationNode(
  node: DiagramEditorNode,
): node is OperationNode {
  return !node.id.startsWith('builtin:');
}

export function extractOperation(node: OperationNode): DiagramOperation {
  const op: DiagramOperation = { ...node.data };
  const opIdKey = 'opId';
  delete op[opIdKey];
  return op;
}
