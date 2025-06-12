import { StepEdge } from '@xyflow/react';
import type { Edge, EdgeTypes } from './types';
import UnzipEdgeComp, { type UnzipEdge } from './unzip-edge';

export type { EdgeTypes } from './types';
export type { UnzipEdge, UnzipEdgeData } from './unzip-edge';

// TODO: Implement all edges
export const EDGE_TYPES = {
  default: StepEdge,
  unzip: UnzipEdgeComp,
  forkResultOk: StepEdge,
  forkResultErr: StepEdge,
  splitKey: StepEdge,
  splitSeq: StepEdge,
  splitRemaining: StepEdge,
  bufferKey: StepEdge,
  bufferSeq: StepEdge,
} satisfies Record<EdgeTypes, unknown>;

export type ForkResultEdgeData = Record<string, never>;
export type ForkResultEdge = Edge<
  ForkResultEdgeData,
  'forkResultOk' | 'forkResultErr'
>;

export type SplitKeyEdgeData = {
  key: string;
};
export type SplitKeyEdge = Edge<SplitKeyEdgeData, 'splitKey'>;

export type SplitSequentialEdgeData = {
  seq: number;
};
export type SplitSeqEdge = Edge<SplitSequentialEdgeData, 'splitSeq'>;

export type SplitRemainingEdgeData = Record<string, never>;
export type SplitRemainingEdge = Edge<SplitRemainingEdgeData, 'splitRemaining'>;

export type BufferKeyEdgeData = {
  key: string;
};
export type BufferKeyEdge = Edge<BufferKeyEdgeData, 'bufferKey'>;

export type BufferSeqEdgeData = {
  seq: number;
};
export type BufferSeq = Edge<BufferSeqEdgeData, 'bufferSeq'>;

export type DiagramEditorEdge =
  | Edge<Record<string, never>, 'default'>
  | UnzipEdge
  | ForkResultEdge
  | SplitKeyEdge
  | SplitSeqEdge
  | SplitRemainingEdge
  | BufferKeyEdge
  | BufferSeq;
