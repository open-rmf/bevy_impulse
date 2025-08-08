import type { NodeProps } from '@xyflow/react';
import type { OperationNode } from '.';
import BaseNode, { HandleType } from './base-node';
import { NodeIcon } from './icons';

function NodeNodeComp(props: NodeProps<OperationNode<'node'>>) {
  return (
    <BaseNode
      {...props}
      icon={<NodeIcon />}
      label={props.data.op.builder ?? ''}
      variant="inputOutput"
      outputHandleType={HandleType.DataStream}
    />
  );
}

export default NodeNodeComp;
