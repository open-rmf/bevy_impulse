import type { NodeProps } from '@xyflow/react';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { BufferIcon } from './icons';

function BufferNode(props: NodeProps<OperationNode<'buffer'>>) {
  return (
    <BaseNode
      {...props}
      icon={<BufferIcon />}
      label="Buffer"
      variant="inputOutput"
    />
  );
}

export default BufferNode;
