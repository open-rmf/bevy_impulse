import { type EdgeProps, StepEdge } from '@xyflow/react';
import type { Edge } from '../types/react-flow';
import type { EdgeData } from '.';

export type BufferKeySlotData = {
  type: 'bufferKey';
  key: string;
};

export type BufferSeqSlotData = {
  type: 'bufferSeq';
  seq: number;
};

export type BufferOutputData = { [k: string]: unknown };
// TODO: support section buffer
export type BufferEdge = Edge<EdgeData<BufferOutputData>, 'buffer'>;

export type BufferEdgeProps = Exclude<EdgeProps<BufferEdge>, 'label'>;

const BufferEdgeComp = StepEdge;

export default BufferEdgeComp;
