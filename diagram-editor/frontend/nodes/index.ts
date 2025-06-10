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

export enum EdgeType {
  Basic,
  Unzip,
  ForkResultOk,
  ForkResultErr,
  SplitKey,
  SplitSequential,
  SplitRemaining,
  BufferKey,
  BufferSeq,
}

export type BasicEdgeData = {
  type: EdgeType.Basic;
};

export type UnzipEdgeData = {
  type: EdgeType.Unzip;
  seq: number;
};

export type ForkResultEdgeData = {
  type: EdgeType.ForkResultOk | EdgeType.ForkResultErr;
};

export type SplitKeyEdgeData = {
  type: EdgeType.SplitKey;
  key: string;
};

export type SplitSequentialEdgeData = {
  type: EdgeType.SplitSequential;
  seq: number;
};

export type SplitRemainingEdgeData = {
  type: EdgeType.SplitRemaining;
};

export type BufferKeyEdgeData = {
  type: EdgeType.BufferKey;
  key: string;
};

export type BufferSeqEdgeData = {
  type: EdgeType.BufferSeq;
  seq: number;
};

export type SplitEdgeData =
  | SplitKeyEdgeData
  | SplitSequentialEdgeData
  | SplitRemainingEdgeData;

export type DiagramEditorEdge = Edge<
  | BasicEdgeData
  | UnzipEdgeData
  | ForkResultEdgeData
  | SplitEdgeData
  | BufferKeyEdgeData
  | BufferSeqEdgeData
>;
