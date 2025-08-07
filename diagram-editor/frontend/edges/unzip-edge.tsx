import { type EdgeProps, StepEdge } from '@xyflow/react';
import { memo } from 'react';
import type { DataEdge } from './data-edge';

export type UnzipOutputData = {
  seq: number;
};

export type UnzipEdge = DataEdge<UnzipOutputData, 'unzip'>;

export type UnzipEdgeProps = Exclude<EdgeProps<UnzipEdge>, 'label'>;

function UnzipEdgeComp(props: UnzipEdgeProps) {
  return <StepEdge {...props} label={props.data.output.seq.toString()} />;
}

export default memo(UnzipEdgeComp);
