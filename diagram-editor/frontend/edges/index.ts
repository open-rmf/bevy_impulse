import { StepEdge } from '@xyflow/react';
import type { EdgeTypes } from '..';
import BufferKeyEdgeComp from './buffer-key-edge';
import BufferSeqEdgeComp from './buffer-seq-edge';
import ForkResultErrEdgeComp from './fork-result-err-edge';
import ForkResultOkEdgeComp from './fork-result-ok-edge';
import SplitKeyEdgeComp from './split-key-edge';
import SplitRemainingEdgeComp from './split-remaining-edge';
import SplitSeqEdgeComp from './split-seq-edge';
import StreamOutEdgeComp from './stream-out-edge';
import UnzipEdgeComp from './unzip-edge';

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
