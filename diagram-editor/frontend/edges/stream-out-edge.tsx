import { type EdgeProps, StepEdge } from '@xyflow/react';
import type { StreamOutEdge } from '..';

export type StreamOutEdgeProps = Exclude<EdgeProps<StreamOutEdge>, 'label'>;

function StreamOutEdgeComp(props: StreamOutEdgeProps) {
  return <StepEdge {...props} label={`stream: ${props.data.name}`} />;
}

export default StreamOutEdgeComp;
