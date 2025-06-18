import { StepEdge } from '@xyflow/react';
import type { BufferKeyEdge } from './buffer-key-edge';
import BufferKeyEdgeComp from './buffer-key-edge';
import type { BufferSeqEdge } from './buffer-seq-edge';
import BufferSeqEdgeComp from './buffer-seq-edge';
import type { ForkResultErrEdge } from './fork-result-err-edge';
import ForkResultErrEdgeComp from './fork-result-err-edge';
import type { ForkResultOkEdge } from './fork-result-ok-edge';
import ForkResultOkEdgeComp from './fork-result-ok-edge';
import type { SplitKeyEdge } from './split-key-edge';
import SplitKeyEdgeComp from './split-key-edge';
import SplitRemainingEdgeComp, {
  type SplitRemainingEdge,
} from './split-remaining-edge';
import type { SplitSeqEdge } from './split-seq-edge';
import SplitSeqEdgeComp from './split-seq-edge';
import StreamOutEdgeComp, { StreamOutEdge } from './stream-out-edge';
import type { Edge, EdgeTypes } from './types';
import UnzipEdgeComp, { type UnzipEdge } from './unzip-edge';

export type { BufferKeyEdge, BufferKeyEdgeData } from './buffer-key-edge';
export type { BufferSeqEdge, BufferSeqEdgeData } from './buffer-seq-edge';
export type {
  ForkResultErrEdge,
  ForkResultErrEdgeData,
} from './fork-result-err-edge';
export type {
  ForkResultOkEdge,
  ForkResultOkEdgeData,
} from './fork-result-ok-edge';
export type { SplitKeyEdge, SplitKeyEdgeData } from './split-key-edge';
export type {
  SplitRemainingEdge,
  SplitRemainingEdgeData,
} from './split-remaining-edge';
export type { SplitSeqEdge, SplitSeqEdgeData } from './split-seq-edge';
export type { StreamOutEdge, StreamOutEdgeData } from './stream-out-edge';
export type { EdgeTypes } from './types';
export type { UnzipEdge, UnzipEdgeData } from './unzip-edge';

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

export type DefaultEdgeData = Record<string, never>;
export type DefaultEdge = Edge<DefaultEdgeData, 'default'>;

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
