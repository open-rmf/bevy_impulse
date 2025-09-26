import { type NodeProps, Position } from '@xyflow/react';
import { Handle, HandleType } from '../handles';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { UnzipIcon } from './icons';

function UnzipNodeComp(props: NodeProps<OperationNode<'unzip'>>) {
  return (
    <BaseNode
      {...props}
      icon={<UnzipIcon />}
      label="Unzip"
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

export default UnzipNodeComp;
