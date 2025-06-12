import type { Edge } from '@xyflow/react';

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
export type BasicEdge = Edge<BasicEdgeData>;

export type UnzipEdgeData = {
  type: EdgeType.Unzip;
  seq: number;
};
export type UnzipEdge = Edge<UnzipEdgeData>;

export type ForkResultEdgeData = {
  type: EdgeType.ForkResultOk | EdgeType.ForkResultErr;
};
export type ForkResultEdge = Edge<ForkResultEdgeData>;

export type SplitKeyEdgeData = {
  type: EdgeType.SplitKey;
  key: string;
};
export type SplitKeyEdge = Edge<SplitKeyEdgeData>;

export type SplitSequentialEdgeData = {
  type: EdgeType.SplitSequential;
  seq: number;
};
export type SplitSequentialEdge = Edge<SplitSequentialEdgeData>;

export type SplitRemainingEdgeData = {
  type: EdgeType.SplitRemaining;
};
export type SplitRemainingEdge = Edge<SplitRemainingEdgeData>;

export type BufferKeyEdgeData = {
  type: EdgeType.BufferKey;
  key: string;
};
export type BufferKeyEdge = Edge<BufferKeyEdgeData>;

export type BufferSeqEdgeData = {
  type: EdgeType.BufferSeq;
  seq: number;
};
export type BufferSeqEdge = Edge<BufferSeqEdgeData>;

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
