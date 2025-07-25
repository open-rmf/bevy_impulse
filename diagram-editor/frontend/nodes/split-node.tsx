import type { NodeProps } from '@xyflow/react';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { SplitIcon } from './icons';

function SplitNode(props: NodeProps<OperationNode<'split'>>) {
  return (
    <BaseNode
      {...props}
      icon={<SplitIcon />}
      label="Split"
      variant="inputOutput"
    />
  );
}

export default SplitNode;
