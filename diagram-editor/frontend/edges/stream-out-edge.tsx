import { type EdgeProps, StepEdge } from '@xyflow/react';
import { memo } from 'react';
import type { Edge } from '../types/react-flow';
import type { DefaultInputSlotData } from './input-slots';

export type StreamOutOutputData = {
  streamId: string;
};

export type StreamOutEdge = Edge<
  StreamOutOutputData,
  DefaultInputSlotData,
  'streamOut'
>;

export type StreamOutEdgeProps = Exclude<EdgeProps<StreamOutEdge>, 'label'>;

function StreamOutEdgeComp(props: StreamOutEdgeProps) {
  return (
    <StepEdge {...props} label={`stream: ${props.data.output.streamId}`} />
  );
}

export default memo(StreamOutEdgeComp);
