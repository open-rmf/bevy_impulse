import { type BufferEdge, BufferEdgeComp } from './buffer-edge';
import type { DataEdge } from './data-edge';
import { type DefaultEdge, DefaultEdgeComp } from './default-edge';
import ForkResultErrEdgeComp, {
  type ForkResultErrEdge,
} from './fork-result-err-edge';
import ForkResultOkEdgeComp, {
  type ForkResultOkEdge,
} from './fork-result-ok-edge';
import { type SectionEdge, SectionOutputEdgeComp } from './section-edge';
import SplitKeyEdgeComp, { type SplitKeyEdge } from './split-key-edge';
import SplitRemainingEdgeComp, {
  type SplitRemainingEdge,
} from './split-remaining-edge';
import SplitSeqEdgeComp, { type SplitSeqEdge } from './split-seq-edge';
import StreamOutEdgeComp, { type StreamOutEdge } from './stream-out-edge';
import UnzipEdgeComp, { type UnzipEdge } from './unzip-edge';

export type { BufferEdge } from './buffer-edge';
export * from './create-edge';
export type * from './input-slots';
export type { SectionEdge } from './section-edge';
export type { SplitKeyEdge } from './split-key-edge';
export type { SplitRemainingEdge } from './split-remaining-edge';
export type { SplitSeqEdge } from './split-seq-edge';
export type { StreamOutEdge } from './stream-out-edge';
export type { UnzipEdge } from './unzip-edge';

type EdgeMapping = {
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

export type EdgeInputData<K extends EdgeTypes = EdgeTypes> =
  EdgeData<K>['input'];

export const EDGE_TYPES = {
  default: DefaultEdgeComp,
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

export enum EdgeCategory {
  Data,
  Buffer,
}

export const EDGE_CATEGORIES = {
  buffer: EdgeCategory.Buffer,
  forkResultOk: EdgeCategory.Data,
  forkResultErr: EdgeCategory.Data,
  splitKey: EdgeCategory.Data,
  splitSeq: EdgeCategory.Data,
  splitRemaining: EdgeCategory.Data,
  default: EdgeCategory.Data,
  streamOut: EdgeCategory.Data,
  unzip: EdgeCategory.Data,
  section: EdgeCategory.Data,
} satisfies Record<EdgeTypes, EdgeCategory>;

export type DataEdgeTypes = {
  [K in EdgeTypes]: (typeof EDGE_CATEGORIES)[K] extends EdgeCategory.Data
    ? K
    : never;
}[EdgeTypes];

export function isDataEdge<T extends EdgeTypes>(
  edge: DiagramEditorEdge<T>,
): edge is DiagramEditorEdge<T> & DataEdge<Record<string, unknown>, T> {
  return EDGE_CATEGORIES[edge.type] === EdgeCategory.Data;
}
