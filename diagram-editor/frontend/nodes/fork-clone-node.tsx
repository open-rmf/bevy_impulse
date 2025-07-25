import type { NodeProps } from '@xyflow/react';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { ForkCloneIcon } from './icons';

function ForkCloneNode(props: NodeProps<OperationNode<'fork_clone'>>) {
  return (
    <BaseNode
      {...props}
      icon={<ForkCloneIcon />}
      label="Fork Clone"
      variant="inputOutput"
    />
  );
}

export default ForkCloneNode;
