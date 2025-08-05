import { type EdgeProps, StepEdge } from '@xyflow/react';
import { memo } from 'react';
import type { DataEdge } from '.';

export type SplitKeyOutputData = {
  key: string;
};

export type SplitKeyEdge = DataEdge<SplitKeyOutputData, 'splitKey'>;

export type SplitKeyEdgeProps = Exclude<EdgeProps<SplitKeyEdge>, 'label'>;

function SplitKeyEdgeComp(props: SplitKeyEdgeProps) {
  return <StepEdge {...props} label={props.data.output.key.toString()} />;
}

export default memo(SplitKeyEdgeComp);
