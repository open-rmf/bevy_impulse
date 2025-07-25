import { type EdgeProps, StepEdge } from '@xyflow/react';
import { memo } from 'react';
import type { Edge } from '../types/react-flow';

export type BufferKeyEdgeData = {
  key: string;
};
export type BufferKeyEdge = Edge<BufferKeyEdgeData, 'bufferKey'>;

export type BufferKeyEdgeProps = Exclude<EdgeProps<BufferKeyEdge>, 'label'>;

function BufferKeyEdgeComp(props: BufferKeyEdgeProps) {
  return <StepEdge {...props} label={props.data.key.toString()} />;
}

export default memo(BufferKeyEdgeComp);
