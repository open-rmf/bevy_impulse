import { type EdgeProps, StepEdge } from '@xyflow/react';
import type { Edge } from './types';

export type BufferSeqEdgeData = {
  seq: number;
};

export type BufferSeqEdge = Edge<BufferSeqEdgeData, 'bufferSeq'>;

export type BufferSeqEdgeProps = Exclude<EdgeProps<BufferSeqEdge>, 'label'>;

function BufferSeqEdgeComp(props: BufferSeqEdgeProps) {
  return <StepEdge {...props} label={props.data.seq.toString()} />;
}

export default BufferSeqEdgeComp;
