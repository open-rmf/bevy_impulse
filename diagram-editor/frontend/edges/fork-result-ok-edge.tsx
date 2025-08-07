import { type EdgeProps, StepEdge } from '@xyflow/react';
import { memo } from 'react';
import type { DataEdge } from './data-edge';

export type ForkResultOkOutputData = Record<string, never>;
export type ForkResultOkEdge = DataEdge<ForkResultOkOutputData, 'forkResultOk'>;

export type ForkResultOkEdgeProps = Exclude<
  EdgeProps<ForkResultOkEdge>,
  'label'
>;

function ForkResultOkEdgeComp(props: ForkResultOkEdgeProps) {
  return <StepEdge {...props} label="ok" />;
}

export default memo(ForkResultOkEdgeComp);
