import { type EdgeProps, StepEdge } from '@xyflow/react';
import type { BufferKeyEdge } from '..';

export type BufferKeyEdgeProps = Exclude<EdgeProps<BufferKeyEdge>, 'label'>;

function BufferKeyEdgeComp(props: BufferKeyEdgeProps) {
  return <StepEdge {...props} label={props.data.key.toString()} />;
}

export default BufferKeyEdgeComp;
