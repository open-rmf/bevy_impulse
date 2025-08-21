import { type NodeProps, Position } from '@xyflow/react';
import { Handle, HandleId, HandleType } from '../handles';
import { useRegistry } from '../registry-provider';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { NodeIcon } from './icons';

function NodeNodeComp(props: NodeProps<OperationNode<'node'>>) {
  const registry = useRegistry();
  const builderMetadata = registry.nodes[props.data.op.builder];
  const label = props.data.op.display_text
    ? props.data.op.display_text
    : builderMetadata
      ? builderMetadata.default_display_text
      : 'Select Builder';

  return (
    <BaseNode
      {...props}
      icon={<NodeIcon />}
      label={label || 'Select Builder'}
      caption={props.data.op.builder}
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
          <Handle
            id={HandleId.DataStream}
            type="source"
            position={Position.Right}
            isConnectable={props.isConnectable}
            variant={HandleType.DataStream}
          />
        </>
      }
    />
  );
}

export default NodeNodeComp;
