import { type NodeProps, Position } from '@xyflow/react';
import { Handle, HandleType } from '../handles';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { ListenIcon } from './icons';

function ListenNodeComp(props: NodeProps<OperationNode<'listen'>>) {
  return (
    <BaseNode
      {...props}
      icon={<ListenIcon />}
      label="Listen"
      handles={
        <>
          <Handle
            type="target"
            position={Position.Top}
            isConnectable={props.isConnectable}
            variant={HandleType.Buffer}
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

export default ListenNodeComp;
