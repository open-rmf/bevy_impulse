import { type EdgeProps, StepEdge } from '@xyflow/react';
import { memo } from 'react';
import type { DataEdge } from '.';

export type SplitRemainingOutputData = Record<string, never>;
export type SplitRemainingEdge = DataEdge<
  SplitRemainingOutputData,
  'splitRemaining'
>;

export type SplitRemainingEdgeProps = Exclude<
  EdgeProps<SplitRemainingEdge>,
  'label'
>;

function SplitRemainingEdgeComp(props: SplitRemainingEdgeProps) {
  return <StepEdge {...props} label="*" />;
}

export default memo(SplitRemainingEdgeComp);
