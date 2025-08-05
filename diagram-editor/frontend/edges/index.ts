import { StepEdge } from '@xyflow/react';
import type { Edge } from '../types/react-flow';
import BufferEdgeComp, { type BufferEdge } from './buffer-edge';
import ForkResultErrEdgeComp, {
  type ForkResultErrEdge,
} from './fork-result-err-edge';
import ForkResultOkEdgeComp, {
  type ForkResultOkEdge,
} from './fork-result-ok-edge';
import {
  type SectionEdge,
  type SectionInputSlotData,
  SectionOutputEdgeComp,
} from './section-edge';
import SplitKeyEdgeComp, { type SplitKeyEdge } from './split-key-edge';
import SplitRemainingEdgeComp, {
  type SplitRemainingEdge,
} from './split-remaining-edge';
import SplitSeqEdgeComp, { type SplitSeqEdge } from './split-seq-edge';
import StreamOutEdgeComp, { type StreamOutEdge } from './stream-out-edge';
import UnzipEdgeComp, { type UnzipEdge } from './unzip-edge';

export type { BufferEdge } from './buffer-edge';
export * from './create-edge';
export type { SplitKeyEdge } from './split-key-edge';
export type { SplitRemainingEdge } from './split-remaining-edge';
export type { SplitSeqEdge } from './split-seq-edge';
export type { StreamOutEdge } from './stream-out-edge';
export type { UnzipEdge } from './unzip-edge';

export type DefaultOutputData = Record<string, never>;
export type DefaultInputData = { type: 'default' };

export type DataEdge<
  O extends Record<string, unknown>,
  S extends string,
> = Edge<O, DefaultInputData | SectionInputSlotData, S>;

export type DefaultEdge = DataEdge<DefaultOutputData, 'default'>;

export type EdgeMapping = {
  default: DefaultEdge;
  unzip: UnzipEdge;
  forkResultOk: ForkResultOkEdge;
  forkResultErr: ForkResultErrEdge;
  splitKey: SplitKeyEdge;
  splitSeq: SplitSeqEdge;
  splitRemaining: SplitRemainingEdge;
  buffer: BufferEdge;
  streamOut: StreamOutEdge;
  section: SectionEdge;
};

export type EdgeTypes = keyof EdgeMapping;

export type EdgeData<K extends EdgeTypes = EdgeTypes> = EdgeMapping[K]['data'];

export type EdgeOutputData<K extends EdgeTypes = EdgeTypes> =
  EdgeData<K>['output'];

export const EDGE_TYPES = {
  default: StepEdge,
  unzip: UnzipEdgeComp,
  forkResultOk: ForkResultOkEdgeComp,
  forkResultErr: ForkResultErrEdgeComp,
  splitKey: SplitKeyEdgeComp,
  splitSeq: SplitSeqEdgeComp,
  splitRemaining: SplitRemainingEdgeComp,
  buffer: BufferEdgeComp,
  streamOut: StreamOutEdgeComp,
  section: SectionOutputEdgeComp,
} satisfies Record<EdgeTypes, unknown>;

export type DiagramEditorEdge<T extends EdgeTypes = EdgeTypes> = EdgeMapping[T];
