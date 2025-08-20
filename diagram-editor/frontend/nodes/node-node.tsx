import { type NodeProps, Position } from '@xyflow/react';
import { HandleType } from '../handles';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { NodeIcon } from './icons';

function NodeNodeComp(props: NodeProps<OperationNode<'node'>>) {
  return (
    <BaseNode
      {...props}
      icon={<NodeIcon />}
      label={props.data.op.builder || 'Select Builder'}
      variant="inputOutput"
      outputHandleType={HandleType.Data}
      extraHandles={[
        {
          position: Position.Right,
          type: 'source',
          variant: HandleType.DataStream,
        },
      ]}
    />
  );
}

export default NodeNodeComp;
