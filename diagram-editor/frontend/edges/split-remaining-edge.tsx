import { type EdgeProps, StepEdge } from '@xyflow/react';
import { memo } from 'react';
import type { SplitRemainingEdge } from '../types';

export type SplitRemainingEdgeProps = Exclude<
  EdgeProps<SplitRemainingEdge>,
  'label'
>;

function SplitRemainingEdgeComp(props: SplitRemainingEdgeProps) {
  return <StepEdge {...props} label="*" />;
}

export default memo(SplitRemainingEdgeComp);
