import type { NodeProps } from '@xyflow/react';
import React from 'react';
import type { DiagramEditorNode } from '../types';
import BaseNode from './base-node';
import { SerializedJoinIcon } from './icons';

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

export default React.memo(SerializedJoinNode);
