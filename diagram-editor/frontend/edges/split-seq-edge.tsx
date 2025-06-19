import { type EdgeProps, StepEdge } from '@xyflow/react';
import type { SplitSeqEdge } from '../types';

export type SplitSeqEdgeProps = Exclude<EdgeProps<SplitSeqEdge>, 'label'>;

function SplitSeqEdgeComp(props: SplitSeqEdgeProps) {
  return <StepEdge {...props} label={props.data.seq.toString()} />;
}

export default SplitSeqEdgeComp;
