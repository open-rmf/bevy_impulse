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
export * from './create-edge';
export type { SplitKeyEdge } from './split-key-edge';
export type { SplitRemainingEdge } from './split-remaining-edge';
export type { SplitSeqEdge } from './split-seq-edge';
export type { StreamOutEdge } from './stream-out-edge';
export type { UnzipEdge } from './unzip-edge';
export type DefaultEdgeData = Record<string, never>;
export type DefaultEdge = Edge<DefaultEdgeData, 'default'>;

type EdgeMapping = {
  default: { edge: DefaultEdge; data: DefaultEdgeData };
  unzip: { edge: UnzipEdge; data: UnzipEdgeData };
  forkResultOk: { edge: ForkResultOkEdge; data: ForkResultOkEdgeData };
  forkResultErr: { edge: ForkResultErrEdge; data: ForkResultErrEdgeData };
  splitKey: { edge: SplitKeyEdge; data: SplitKeyEdgeData };
  splitSeq: { edge: SplitSeqEdge; data: SplitSeqEdgeData };
  splitRemaining: { edge: SplitRemainingEdge; data: SplitRemainingEdgeData };
  bufferKey: { edge: BufferKeyEdge; data: BufferKeyEdgeData };
  bufferSeq: { edge: BufferSeqEdge; data: BufferSeqEdgeData };
  streamOut: { edge: StreamOutEdge; data: StreamOutEdgeData };
};

export type EdgeTypes = keyof EdgeMapping;

export type DiagramEditorEdge<T extends EdgeTypes = EdgeTypes> =
  EdgeMapping[T]['edge'];

export type EdgeData<T extends EdgeTypes = EdgeTypes> = EdgeMapping[T]['data'];

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
} satisfies Record<EdgeTypes, unknown>;
