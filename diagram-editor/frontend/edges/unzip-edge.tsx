import { type EdgeProps, StepEdge } from '@xyflow/react';
import type { Edge } from './types';

export type UnzipEdgeData = {
  seq: number;
};

export type UnzipEdge = Edge<UnzipEdgeData, 'unzip'>;

export type UnzipEdgeProps = Exclude<EdgeProps<UnzipEdge>, 'label'>;

function UnzipEdgeComp(props: UnzipEdgeProps) {
  return <StepEdge {...props} label={props.data.seq.toString()} />;
}

export default UnzipEdgeComp;
