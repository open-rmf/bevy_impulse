import { type EdgeProps, StepEdge } from '@xyflow/react';
import type { UnzipEdge } from '..';

export type UnzipEdgeProps = Exclude<EdgeProps<UnzipEdge>, 'label'>;

function UnzipEdgeComp(props: UnzipEdgeProps) {
  return <StepEdge {...props} label={props.data.seq.toString()} />;
}

export default UnzipEdgeComp;
