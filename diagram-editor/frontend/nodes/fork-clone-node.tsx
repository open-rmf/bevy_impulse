import type { NodeProps } from '@xyflow/react';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { ForkCloneIcon } from './icons';

function ForkCloneNodeComp(props: NodeProps<OperationNode<'fork_clone'>>) {
  return (
    <BaseNode
      {...props}
      icon={<ForkCloneIcon />}
      label="Fork Clone"
      variant="inputOutput"
    />
  );
}

export default ForkCloneNodeComp;
