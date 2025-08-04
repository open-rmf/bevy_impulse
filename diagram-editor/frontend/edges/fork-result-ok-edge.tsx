import { type EdgeProps, StepEdge } from '@xyflow/react';
import { memo } from 'react';
import type { Edge } from '../types/react-flow';

export type ForkResultOkOutputData = Record<string, never>;
export type ForkResultOkEdge = Edge<ForkResultOkOutputData, 'forkResultOk'>;

export type ForkResultOkEdgeProps = Exclude<
  EdgeProps<ForkResultOkEdge>,
  'label'
>;

function ForkResultOkEdgeComp(props: ForkResultOkEdgeProps) {
  return <StepEdge {...props} label="ok" />;
}

export default memo(ForkResultOkEdgeComp);
