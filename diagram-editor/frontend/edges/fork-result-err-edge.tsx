import { type EdgeProps, StepEdge } from '@xyflow/react';
import type { ForkResultErrEdge } from '..';

export type ForkResultErrEdgeProps = Exclude<
  EdgeProps<ForkResultErrEdge>,
  'label'
>;

function ForkResultErrEdgeComp(props: ForkResultErrEdgeProps) {
  return <StepEdge {...props} label="error" />;
}

export default ForkResultErrEdgeComp;
