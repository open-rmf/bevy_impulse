import { type EdgeProps, StepEdge } from '@xyflow/react';
import type { Edge } from './types';

export type StreamOutEdgeData = {
  name: string;
};

export type StreamOutEdge = Edge<StreamOutEdgeData, 'streamOut'>;

export type StreamOutEdgeProps = Exclude<EdgeProps<StreamOutEdge>, 'label'>;

function StreamOutEdgeComp(props: StreamOutEdgeProps) {
  return <StepEdge {...props} label={`stream: ${props.data.name}`} />;
}

export default StreamOutEdgeComp;
