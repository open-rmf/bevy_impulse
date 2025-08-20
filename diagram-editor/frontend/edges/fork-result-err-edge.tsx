import { type EdgeProps, StepEdge } from '@xyflow/react';
import { memo } from 'react';
import type { DataEdge } from './data-edge';

export type ForkResultErrOutputData = Record<string, never>;
export type ForkResultErrEdge = DataEdge<
  ForkResultErrOutputData,
  'forkResultErr'
>;

export type ForkResultErrEdgeProps = Exclude<
  EdgeProps<ForkResultErrEdge>,
  'label'
>;

function ForkResultErrEdgeComp(props: ForkResultErrEdgeProps) {
  return <StepEdge {...props} label="error" />;
}

export default memo(ForkResultErrEdgeComp);
