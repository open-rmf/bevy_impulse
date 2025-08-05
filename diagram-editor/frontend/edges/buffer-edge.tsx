import { type EdgeProps, StepEdge } from '@xyflow/react';
import type { Edge } from '../types/react-flow';
import type { SectionBufferSlotData } from './section-edge';

export type BufferKeySlotData = {
  type: 'bufferKey';
  key: string;
};

export type BufferSeqSlotData = {
  type: 'bufferSeq';
  seq: number;
};

export type BufferOutputData = Record<string, never>;

export type BufferEdge = Edge<
  BufferOutputData,
  BufferKeySlotData | BufferSeqSlotData | SectionBufferSlotData,
  'buffer'
>;

export type BufferEdgeProps = Exclude<EdgeProps<BufferEdge>, 'label'>;

const BufferEdgeComp = StepEdge;

export default BufferEdgeComp;
