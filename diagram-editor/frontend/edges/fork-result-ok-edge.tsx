import { type EdgeProps, StepEdge } from '@xyflow/react';
import type { ForkResultOkEdge } from '..';

export type ForkResultOkEdgeProps = Exclude<
  EdgeProps<ForkResultOkEdge>,
  'label'
>;

function ForkResultOkEdgeComp(props: ForkResultOkEdgeProps) {
  return <StepEdge {...props} label="ok" />;
}

export default ForkResultOkEdgeComp;
