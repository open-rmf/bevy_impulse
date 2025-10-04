import { type NodeProps, Position } from '@xyflow/react';
import { Handle, HandleType } from '../handles';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { ForkCloneIcon } from './icons';

function ForkCloneNodeComp(props: NodeProps<OperationNode<'fork_clone'>>) {
  return (
    <BaseNode
      {...props}
      icon={<ForkCloneIcon />}
      label="Fork Clone"
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

export default ForkCloneNodeComp;
