import { StartNode } from './start-node';
import { TerminateNode } from './terminate-node';

import type { Node } from '@xyflow/react';
import type { DiagramOperation } from '../types/diagram';
import { InputOutputNode } from './input-output-node';
import { OutputNode } from './output-node';

export const START_ID = 'builtin:start';
export const TERMINATE_ID = 'builtin:terminate';

export const NODE_TYPES = {
  start: StartNode,
  terminate: TerminateNode,
  inputOutput: InputOutputNode,
  output: OutputNode,
};

export type NodeTypes = keyof typeof NODE_TYPES;

export type BuiltinNode = Node<Record<string, never>, NodeTypes>;

export type OperationNode = Node<
  DiagramOperation & { opId: string },
  NodeTypes
>;

export type DiagramEditorNode = BuiltinNode | OperationNode;

type JoinOperation = Extract<
  DiagramOperation,
  { type: 'join' } | { type: 'serialized_join' }
>;

export type JoinNode = Node<JoinOperation, NodeTypes>;

export function isOperationNode(
  node: DiagramEditorNode,
): node is OperationNode {
  return !node.id.startsWith('builtin:');
}
