import type { NodeProps } from '@xyflow/react';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { ForkResultIcon } from './icons';

function ForkResultNodeComp(props: NodeProps<OperationNode<'fork_result'>>) {
  return (
    <BaseNode
      {...props}
      icon={<ForkResultIcon />}
      label="Fork Result"
      variant="inputOutput"
    />
  );
}

export default ForkResultNodeComp;
