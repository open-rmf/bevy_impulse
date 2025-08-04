import { type EdgeProps, StepEdge } from '@xyflow/react';
import { memo } from 'react';
import type { Edge } from '../types/react-flow';

export type SplitRemainingOutputData = Record<string, never>;
export type SplitRemainingEdge = Edge<
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
