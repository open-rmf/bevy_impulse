import { type EdgeProps, StepEdge } from '@xyflow/react';
import { memo } from 'react';
import type { Edge } from '../types/react-flow';

export type ForkResultErrEdgeData = Record<string, never>;
export type ForkResultErrEdge = Edge<ForkResultErrEdgeData, 'forkResultErr'>;

export type ForkResultErrEdgeProps = Exclude<
  EdgeProps<ForkResultErrEdge>,
  'label'
>;

function ForkResultErrEdgeComp(props: ForkResultErrEdgeProps) {
  return <StepEdge {...props} label="error" />;
}

export default memo(ForkResultErrEdgeComp);
