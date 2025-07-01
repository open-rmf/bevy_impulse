import type { NodeProps } from '@xyflow/react';
import React from 'react';
import type { OperationNode } from '../types';
import BaseNode from './base-node';
import { NodeIcon } from './icons';

function NodeNode(props: NodeProps<OperationNode<'node'>>) {
  return (
    <BaseNode
      {...props}
      icon={<NodeIcon />}
      label={props.data.op.builder}
      variant="inputOutput"
    />
  );
}

export default React.memo(NodeNode);
