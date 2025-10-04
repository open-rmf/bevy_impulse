import { type NodeProps, Position } from '@xyflow/react';
import { Handle, HandleType } from '../handles';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { BufferAccessIcon } from './icons';

function BufferAccessNodeComp(
  props: NodeProps<OperationNode<'buffer_access'>>,
) {
  return (
    <BaseNode
      {...props}
      icon={<BufferAccessIcon />}
      label="Buffer Access"
      handles={
        <>
          <Handle
            type="target"
            position={Position.Top}
            isConnectable={props.isConnectable}
            variant={HandleType.DataBuffer}
          />
          <Handle
            type="source"
            position={Position.Bottom}
            isConnectable={props.isConnectable}
            variant={HandleType.Data}
          />
        </>
      }
    />
  );
}

export default BufferAccessNodeComp;
