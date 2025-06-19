import type { Node as ReactFlowNode } from '@xyflow/react';
import type { DiagramOperation } from './diagram';

export type BuiltinNodeTypes = 'start' | 'terminate';

export type OperationNodeTypes = DiagramOperation['type'];

export type NodeTypes = BuiltinNodeTypes | OperationNodeTypes;

export type Node<
  D extends Record<string, unknown>,
  T extends NodeTypes,
> = ReactFlowNode<D, T>;

export type BuiltinNode = Node<Record<string, never>, BuiltinNodeTypes>;

export type OperationNode<K extends OperationNodeTypes = OperationNodeTypes> =
  Node<Extract<DiagramOperation, { type: K }>, K>;

export type DiagramEditorNode = BuiltinNode | OperationNode;

import type { Edge as ReactFlowEdge } from '@xyflow/react';

export type Edge<
  D extends Record<string, unknown>,
  T extends EdgeTypes,
> = ReactFlowEdge<D, T> & Pick<Required<ReactFlowEdge>, 'type' | 'data'>;

export type DefaultEdgeData = Record<string, never>;
export type DefaultEdge = Edge<DefaultEdgeData, 'default'>;

export type BufferKeyEdgeData = {
  key: string;
};
export type BufferKeyEdge = Edge<BufferKeyEdgeData, 'bufferKey'>;

export type BufferSeqEdgeData = {
  seq: number;
};
export type BufferSeqEdge = Edge<BufferSeqEdgeData, 'bufferSeq'>;

export type ForkResultErrEdgeData = Record<string, never>;
export type ForkResultErrEdge = Edge<ForkResultErrEdgeData, 'forkResultErr'>;

export type ForkResultOkEdgeData = Record<string, never>;
export type ForkResultOkEdge = Edge<ForkResultOkEdgeData, 'forkResultOk'>;

export type SplitKeyEdgeData = {
  key: string;
};
export type SplitKeyEdge = Edge<SplitKeyEdgeData, 'splitKey'>;

export type SplitRemainingEdgeData = Record<string, never>;
export type SplitRemainingEdge = Edge<SplitRemainingEdgeData, 'splitRemaining'>;

export type SplitSeqEdgeData = {
  seq: number;
};
export type SplitSeqEdge = Edge<SplitSeqEdgeData, 'splitSeq'>;

export type StreamOutEdgeData = {
  name: string;
};
export type StreamOutEdge = Edge<StreamOutEdgeData, 'streamOut'>;

export type UnzipEdgeData = {
  seq: number;
};
export type UnzipEdge = Edge<UnzipEdgeData, 'unzip'>;

export type EdgeData = {
  default: DefaultEdgeData;
  unzip: UnzipEdgeData;
  forkResultOk: ForkResultOkEdgeData;
  forkResultErr: ForkResultErrEdgeData;
  splitKey: SplitKeyEdgeData;
  splitSeq: SplitSeqEdgeData;
  splitRemaining: SplitRemainingEdgeData;
  bufferKey: BufferKeyEdgeData;
  bufferSeq: BufferSeqEdgeData;
  streamOut: StreamOutEdgeData;
};

export type EdgeTypes = keyof EdgeData;

export type DiagramEditorEdge =
  | DefaultEdge
  | UnzipEdge
  | ForkResultOkEdge
  | ForkResultErrEdge
  | SplitKeyEdge
  | SplitSeqEdge
  | SplitRemainingEdge
  | BufferKeyEdge
  | BufferSeqEdge
  | StreamOutEdge;
