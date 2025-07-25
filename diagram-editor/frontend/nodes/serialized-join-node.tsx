import type { NodeProps } from '@xyflow/react';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { SerializedJoinIcon } from './icons';

function SerializedJoinNode(
  props: NodeProps<OperationNode<'serialized_join'>>,
) {
  return (
    <BaseNode
      {...props}
      icon={<SerializedJoinIcon />}
      label="Serialized Join"
      variant="inputOutput"
    />
  );
}

export default SerializedJoinNode;
