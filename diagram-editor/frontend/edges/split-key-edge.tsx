import { type EdgeProps, StepEdge } from '@xyflow/react';
import { memo } from 'react';
import type { Edge } from '../types/react-flow';

export type SplitKeyEdgeData = {
  key: string;
};
export type SplitKeyEdge = Edge<SplitKeyEdgeData, 'splitKey'>;

export type SplitKeyEdgeProps = Exclude<EdgeProps<SplitKeyEdge>, 'label'>;

function SplitKeyEdgeComp(props: SplitKeyEdgeProps) {
  return <StepEdge {...props} label={props.data.key.toString()} />;
}

export default memo(SplitKeyEdgeComp);
