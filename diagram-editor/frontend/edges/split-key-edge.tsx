import { type EdgeProps, StepEdge } from '@xyflow/react';
import type { SplitKeyEdge } from '..';

export type SplitKeyEdgeProps = Exclude<EdgeProps<SplitKeyEdge>, 'label'>;

function SplitKeyEdgeComp(props: SplitKeyEdgeProps) {
  return <StepEdge {...props} label={props.data.key.toString()} />;
}

export default SplitKeyEdgeComp;
