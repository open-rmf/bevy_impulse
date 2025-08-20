import type { NodeProps } from '@xyflow/react';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { TransformIcon } from './icons';

function TransformNodeComp(props: NodeProps<OperationNode<'transform'>>) {
  return (
    <BaseNode
      {...props}
      icon={<TransformIcon />}
      label="Transform"
      variant="inputOutput"
    />
  );
}

export default TransformNodeComp;
