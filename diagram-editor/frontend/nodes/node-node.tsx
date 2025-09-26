import {
  type NodeProps,
  Position,
  useUpdateNodeInternals,
} from '@xyflow/react';
import { useRef } from 'react';
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

  const hasStreams = !!(
    builderMetadata?.streams && Object.keys(builderMetadata.streams).length > 0
  );
  const prevHasStreams = useRef(hasStreams);
  const updateNodeInternals = useUpdateNodeInternals();
  if (hasStreams !== prevHasStreams.current) {
    prevHasStreams.current = hasStreams;
    updateNodeInternals(props.id);
  }

  return (
    <BaseNode
      {...props}
      icon={<NodeIcon />}
      label={label || 'Select Builder'}
      caption={props.data.op.builder}
      handles={
        <>
          <Handle
            key="data-target"
            type="target"
            position={Position.Top}
            isConnectable={props.isConnectable}
            variant={HandleType.Data}
          />
          <Handle
            key="data-source"
            type="source"
            position={Position.Bottom}
            isConnectable={props.isConnectable}
            variant={HandleType.Data}
          />
          {hasStreams && (
            <Handle
              key="stream-source"
              id={HandleId.DataStream}
              type="source"
              position={Position.Right}
              isConnectable={props.isConnectable}
              variant={HandleType.DataStream}
            />
          )}
        </>
      }
    />
  );
}

export default NodeNodeComp;
