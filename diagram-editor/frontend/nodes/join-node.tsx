import { type NodeProps, Position } from '@xyflow/react';
import { Handle, HandleType } from '../handles';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { JoinIcon } from './icons';

function JoinNodeComp(props: NodeProps<OperationNode<'join'>>) {
  return (
    <BaseNode
      {...props}
      icon={<JoinIcon />}
      label="Join"
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

export default JoinNodeComp;
