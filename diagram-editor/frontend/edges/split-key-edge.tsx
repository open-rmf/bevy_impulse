import { type EdgeProps, StepEdge } from '@xyflow/react';
import { memo } from 'react';
import type { Edge } from '../types/react-flow';
import type { EdgeData } from '.';

export type SplitKeyOutputData = {
  key: string;
};
export type SplitKeyEdge = Edge<EdgeData<SplitKeyOutputData>, 'splitKey'>;

export type SplitKeyEdgeProps = Exclude<EdgeProps<SplitKeyEdge>, 'label'>;

function SplitKeyEdgeComp(props: SplitKeyEdgeProps) {
  return <StepEdge {...props} label={props.data.output.key.toString()} />;
}

export default memo(SplitKeyEdgeComp);
