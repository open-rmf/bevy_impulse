import { type NodeProps, Position } from '@xyflow/react';
import { Handle, HandleType } from '../handles';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { TransformIcon } from './icons';

function TransformNodeComp(props: NodeProps<OperationNode<'transform'>>) {
  return (
    <BaseNode
      {...props}
      icon={<TransformIcon />}
      label="Transform"
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
            variant={HandleType.Data}
          />
        </>
      }
    />
  );
}

export default TransformNodeComp;
