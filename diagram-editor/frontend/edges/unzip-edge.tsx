import { type EdgeProps, StepEdge } from '@xyflow/react';
import { memo } from 'react';
import type { Edge } from '../types/react-flow';
import type { EdgeData } from '.';

export type UnzipOutputData = {
  seq: number;
};

export type UnzipEdge = Edge<EdgeData<UnzipOutputData>, 'unzip'>;

export type UnzipEdgeProps = Exclude<EdgeProps<UnzipEdge>, 'label'>;

function UnzipEdgeComp(props: UnzipEdgeProps) {
  return <StepEdge {...props} label={props.data.output.seq.toString()} />;
}

export default memo(UnzipEdgeComp);
