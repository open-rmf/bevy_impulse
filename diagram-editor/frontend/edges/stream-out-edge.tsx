import { type EdgeProps, StepEdge } from '@xyflow/react';
import { memo } from 'react';
import type { StreamOutEdge } from '../types';

export type StreamOutEdgeProps = Exclude<EdgeProps<StreamOutEdge>, 'label'>;

function StreamOutEdgeComp(props: StreamOutEdgeProps) {
  return <StepEdge {...props} label={`stream: ${props.data.name}`} />;
}

export default memo(StreamOutEdgeComp);
