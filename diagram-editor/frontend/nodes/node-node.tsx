import { type NodeProps, Position } from '@xyflow/react';
import { HandleType } from '../handles';
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
      variant="inputOutput"
      outputHandleType={HandleType.Data}
      extraHandles={[
        {
          position: Position.Right,
          type: 'source',
          variant: HandleType.DataStream,
        },
      ]}
    />
  );
}

export default NodeNodeComp;
