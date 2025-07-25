import { type EdgeProps, StepEdge } from '@xyflow/react';
import { memo } from 'react';
import type { Edge } from '../types/react-flow';

export type SplitSeqEdgeData = {
  seq: number;
};
export type SplitSeqEdge = Edge<SplitSeqEdgeData, 'splitSeq'>;

export type SplitSeqEdgeProps = Exclude<EdgeProps<SplitSeqEdge>, 'label'>;

function SplitSeqEdgeComp(props: SplitSeqEdgeProps) {
  return <StepEdge {...props} label={props.data.seq.toString()} />;
}

export default memo(SplitSeqEdgeComp);
