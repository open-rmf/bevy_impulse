import type { NodeProps } from '@xyflow/react';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { NodeIcon } from './icons';

function NodeNodeComp(props: NodeProps<OperationNode<'node'>>) {
  return (
    <BaseNode
      {...props}
      icon={<NodeIcon />}
      label={props.data.op.builder ?? ''}
      variant="inputOutput"
    />
  );
}

export default NodeNodeComp;
