import { type EdgeProps, StepEdge } from '@xyflow/react';
import type { SplitRemainingEdge } from '..';

export type SplitRemainingEdgeProps = Exclude<
  EdgeProps<SplitRemainingEdge>,
  'label'
>;

function SplitRemainingEdgeComp(props: SplitRemainingEdgeProps) {
  return <StepEdge {...props} label="*" />;
}

export default SplitRemainingEdgeComp;
