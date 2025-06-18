import type { NodeProps } from '@xyflow/react';
import BaseNode from './base-node';
import { NodeIcon } from './icons';
import type { OperationNode } from './types';

function NodeNode(props: NodeProps<OperationNode<'node'>>) {
  return (
    <BaseNode
      {...props}
      icon={<NodeIcon />}
      label={props.data.builder}
      variant="inputOutput"
    />
  );
}

export default NodeNode;
