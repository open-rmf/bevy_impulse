import { type EdgeProps, StepEdge } from '@xyflow/react';
import { memo } from 'react';
import type { Edge } from '../types/react-flow';
import type { EdgeData } from '.';

export type SplitSeqOutputData = {
  seq: number;
};

export type SplitSeqEdge = Edge<EdgeData<SplitSeqOutputData>, 'splitSeq'>;

export type SplitSeqEdgeProps = Exclude<EdgeProps<SplitSeqEdge>, 'label'>;

function SplitSeqEdgeComp(props: SplitSeqEdgeProps) {
  return <StepEdge {...props} label={props.data.output.seq.toString()} />;
}

export default memo(SplitSeqEdgeComp);
