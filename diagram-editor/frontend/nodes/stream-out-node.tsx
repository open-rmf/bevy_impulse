import type { NodeProps } from '@xyflow/react';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { StreamOutIcon } from './icons';

function StreamOutNode(props: NodeProps<OperationNode<'stream_out'>>) {
  return (
    <BaseNode
      {...props}
      icon={<StreamOutIcon />}
      label="StreamOut"
      variant="input"
    />
  );
}

export default StreamOutNode;
