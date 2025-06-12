import { type EdgeProps, StepEdge } from '@xyflow/react';
import type { Edge } from './types';

export type ForkResultErrEdgeData = Record<string, never>;

export type ForkResultErrEdge = Edge<ForkResultErrEdgeData, 'forkResultErr'>;

export type ForkResultErrEdgeProps = Exclude<
  EdgeProps<ForkResultErrEdge>,
  'label'
>;

function ForkResultErrEdgeComp(props: ForkResultErrEdgeProps) {
  return <StepEdge {...props} label="error" />;
}

export default ForkResultErrEdgeComp;
