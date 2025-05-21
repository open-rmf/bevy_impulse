import { StartNode } from './start-node';
import { TerminateNode } from './terminate-node';

import type { Node } from '@xyflow/react';
import type { DiagramOperation } from '../types/diagram';
import { InputOutputNode } from './input-output-node';
import { OutputNode } from './output-node';

export const NODE_TYPES = {
  start: StartNode,
  terminate: TerminateNode,
  inputOutput: InputOutputNode,
  output: OutputNode,
};

export type NodeTypes = keyof typeof NODE_TYPES;

export type DiagramEditorNode = Node<
  DiagramOperation | Record<string, never>,
  NodeTypes
>;

type JoinOperation = Extract<
  DiagramOperation,
  { type: 'join' } | { type: 'serialized_join' }
>;

export type JoinNode = Node<JoinOperation, NodeTypes>;
