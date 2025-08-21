import { type NodeProps, Position } from '@xyflow/react';
import { Handle, HandleId, HandleType } from '../handles';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { StreamOutIcon } from './icons';

function StreamOutNodeComp(props: NodeProps<OperationNode<'stream_out'>>) {
  return (
    <BaseNode
      {...props}
      icon={<StreamOutIcon />}
      label="StreamOut"
      caption={props.data.op.name}
      handles={
        <Handle
          id={HandleId.DataStream}
          type="target"
          position={Position.Top}
          isConnectable={props.isConnectable}
          variant={HandleType.DataStream}
        />
      }
    />
  );
}

export default StreamOutNodeComp;
