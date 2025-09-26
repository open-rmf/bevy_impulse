import { type NodeProps, Position } from '@xyflow/react';
import { Handle, HandleType } from '../handles';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { BufferIcon } from './icons';

function BufferNodeComp(props: NodeProps<OperationNode<'buffer'>>) {
  return (
    <BaseNode
      {...props}
      icon={<BufferIcon />}
      label="Buffer"
      handles={
        <>
          <Handle
            type="target"
            position={Position.Top}
            isConnectable={props.isConnectable}
            variant={HandleType.Data}
          />
          <Handle
            type="source"
            position={Position.Bottom}
            isConnectable={props.isConnectable}
            variant={HandleType.Buffer}
          />
        </>
      }
    />
  );
}

export default BufferNodeComp;
