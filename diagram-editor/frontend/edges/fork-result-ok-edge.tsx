import { type EdgeProps, StepEdge } from '@xyflow/react';
import type { Edge } from './types';

export type ForkResultOkEdgeData = Record<string, never>;

export type ForkResultOkEdge = Edge<ForkResultOkEdgeData, 'forkResultOk'>;

export type ForkResultOkEdgeProps = Exclude<
  EdgeProps<ForkResultOkEdge>,
  'label'
>;

function ForkResultOkEdgeComp(props: ForkResultOkEdgeProps) {
  return <StepEdge {...props} label="ok" />;
}

export default ForkResultOkEdgeComp;
