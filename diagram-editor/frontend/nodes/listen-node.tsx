import type { NodeProps } from '@xyflow/react';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { ListenIcon } from './icons';

function ListenNode(props: NodeProps<OperationNode<'listen'>>) {
  return (
    <BaseNode
      {...props}
      icon={<ListenIcon />}
      label="Listen"
      variant="inputOutput"
    />
  );
}

export default ListenNode;
