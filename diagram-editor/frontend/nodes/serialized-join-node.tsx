import type { NodeProps } from '@xyflow/react';
import BaseNode from './base-node';
import { SerializedJoinIcon } from './icons';
import type { DiagramEditorNode } from './types';

function SerializedJoinNode(props: NodeProps<DiagramEditorNode>) {
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
