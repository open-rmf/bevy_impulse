import { type EdgeProps, StepEdge } from '@xyflow/react';
import { memo } from 'react';
import type { Edge } from '../types/react-flow';
import type { EdgeData } from '.';

export type StreamOutOutputData = {
  name: string;
};

export type StreamOutEdge = Edge<EdgeData<StreamOutOutputData>, 'streamOut'>;

export type StreamOutEdgeProps = Exclude<EdgeProps<StreamOutEdge>, 'label'>;

function StreamOutEdgeComp(props: StreamOutEdgeProps) {
  return <StepEdge {...props} label={`stream: ${props.data.name}`} />;
}

export default memo(StreamOutEdgeComp);
