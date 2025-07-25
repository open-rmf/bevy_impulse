import type { NodeProps } from '@xyflow/react';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { JoinIcon } from './icons';

function JoinNode(props: NodeProps<OperationNode<'join'>>) {
  return (
    <BaseNode
      {...props}
      icon={<JoinIcon />}
      label="Join"
      variant="inputOutput"
    />
  );
}

export default JoinNode;
