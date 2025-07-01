import { type EdgeProps, StepEdge } from '@xyflow/react';
import { memo } from 'react';
import type { UnzipEdge } from '../types';

export type UnzipEdgeProps = Exclude<EdgeProps<UnzipEdge>, 'label'>;

function UnzipEdgeComp(props: UnzipEdgeProps) {
  return <StepEdge {...props} label={props.data.seq.toString()} />;
}

export default memo(UnzipEdgeComp);
