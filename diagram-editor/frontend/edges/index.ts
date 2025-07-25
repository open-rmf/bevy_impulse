import { StepEdge } from '@xyflow/react';
import type { Edge } from '../types/react-flow';
import BufferKeyEdgeComp, {
  type BufferKeyEdge,
  type BufferKeyEdgeData,
} from './buffer-key-edge';
import BufferSeqEdgeComp, {
  type BufferSeqEdge,
  type BufferSeqEdgeData,
} from './buffer-seq-edge';
import ForkResultErrEdgeComp, {
  type ForkResultErrEdge,
  type ForkResultErrEdgeData,
} from './fork-result-err-edge';
import ForkResultOkEdgeComp, {
  type ForkResultOkEdge,
  type ForkResultOkEdgeData,
} from './fork-result-ok-edge';
import SplitKeyEdgeComp, {
  type SplitKeyEdge,
  type SplitKeyEdgeData,
} from './split-key-edge';
import SplitRemainingEdgeComp, {
  type SplitRemainingEdge,
  type SplitRemainingEdgeData,
} from './split-remaining-edge';
import SplitSeqEdgeComp, {
  type SplitSeqEdge,
  type SplitSeqEdgeData,
} from './split-seq-edge';
import StreamOutEdgeComp, {
  type StreamOutEdge,
  type StreamOutEdgeData,
} from './stream-out-edge';
import UnzipEdgeComp, {
  type UnzipEdge,
  type UnzipEdgeData,
} from './unzip-edge';

export type { BufferKeyEdge } from './buffer-key-edge';
export type { BufferSeqEdge } from './buffer-seq-edge';
export type { SplitKeyEdge } from './split-key-edge';
export type { SplitSeqEdge } from './split-seq-edge';
export type { SplitRemainingEdge } from './split-remaining-edge';
export type { StreamOutEdge } from './stream-out-edge';
export type { UnzipEdge } from './unzip-edge';

export const EDGE_TYPES = {
  default: StepEdge,
  unzip: UnzipEdgeComp,
  forkResultOk: ForkResultOkEdgeComp,
  forkResultErr: ForkResultErrEdgeComp,
  splitKey: SplitKeyEdgeComp,
  splitSeq: SplitSeqEdgeComp,
  splitRemaining: SplitRemainingEdgeComp,
  bufferKey: BufferKeyEdgeComp,
  bufferSeq: BufferSeqEdgeComp,
  streamOut: StreamOutEdgeComp,
};

export type EdgeTypes = keyof typeof EDGE_TYPES;

export type DefaultEdgeData = Record<string, never>;
export type DefaultEdge = Edge<DefaultEdgeData, 'default'>;

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
