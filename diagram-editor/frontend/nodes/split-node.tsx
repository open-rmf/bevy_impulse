import { type NodeProps, Position } from '@xyflow/react';
import { Handle, HandleType } from '../handles';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { SplitIcon } from './icons';

function SplitNodeComp(props: NodeProps<OperationNode<'split'>>) {
  return (
    <BaseNode
      {...props}
      icon={<SplitIcon />}
      label="Split"
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

export default SplitNodeComp;
