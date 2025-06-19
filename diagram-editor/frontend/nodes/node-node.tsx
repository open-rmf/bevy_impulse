import type { NodeProps } from '@xyflow/react';
import type { OperationNode } from '../types';
import BaseNode from './base-node';
import { NodeIcon } from './icons';

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
