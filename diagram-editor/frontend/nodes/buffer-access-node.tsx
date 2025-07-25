import type { NodeProps } from '@xyflow/react';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { BufferAccessIcon } from './icons';

function BufferAccessNode(props: NodeProps<OperationNode<'buffer_access'>>) {
  return (
    <BaseNode
      {...props}
      icon={<BufferAccessIcon />}
      label="Buffer Access"
      variant="inputOutput"
    />
  );
}

export default BufferAccessNode;
