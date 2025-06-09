import { StartNode } from './start-node';
import { TerminateNode } from './terminate-node';

import type { Edge, Node } from '@xyflow/react';
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

export function extractOperation(node: OperationNode): DiagramOperation {
  const op: DiagramOperation = { ...node.data };
  const opIdKey = 'opId';
  delete op[opIdKey];
  return op;
}

export type BasicEdgeData = {
  type: 'basic';
};

export type UnzipEdgeData = {
  type: 'unzip';
  seq: number;
};

export type ForkResultEdgeData = {
  type: 'ok' | 'err';
};

export type SplitKeyEdgeData = {
  type: 'splitKey';
  key: string;
};

export type SplitSequentialEdgeData = {
  type: 'splitSequential';
  seq: number;
};

export type SplitRemainingEdgeData = {
  type: 'splitRemaining';
};

export type SplitEdgeData =
  | SplitKeyEdgeData
  | SplitSequentialEdgeData
  | SplitRemainingEdgeData;

export type DiagramEditorEdge = Edge<
  BasicEdgeData | UnzipEdgeData | ForkResultEdgeData | SplitEdgeData
>;
