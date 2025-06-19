import { type EdgeProps, StepEdge } from '@xyflow/react';
import type { BufferSeqEdge } from '../types';

export type BufferSeqEdgeProps = Exclude<EdgeProps<BufferSeqEdge>, 'label'>;

function BufferSeqEdgeComp(props: BufferSeqEdgeProps) {
  return <StepEdge {...props} label={props.data.seq.toString()} />;
}

export default BufferSeqEdgeComp;
