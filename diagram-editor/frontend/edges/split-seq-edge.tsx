import { type EdgeProps, StepEdge } from '@xyflow/react';
import { memo } from 'react';
import type { DataEdge } from '.';

export type SplitSeqOutputData = {
  seq: number;
};

export type SplitSeqEdge = DataEdge<SplitSeqOutputData, 'splitSeq'>;

export type SplitSeqEdgeProps = Exclude<EdgeProps<SplitSeqEdge>, 'label'>;

function SplitSeqEdgeComp(props: SplitSeqEdgeProps) {
  return <StepEdge {...props} label={props.data.output.seq.toString()} />;
}

export default memo(SplitSeqEdgeComp);
