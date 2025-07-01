import type { NodeProps } from '@xyflow/react';
import React from 'react';
import type { DiagramEditorNode } from '../types';
import BaseNode from './base-node';
import { JoinIcon } from './icons';

function JoinNode(props: NodeProps<DiagramEditorNode>) {
  return (
    <BaseNode
      {...props}
      icon={<JoinIcon />}
      label="Join"
      variant="inputOutput"
    />
  );
}

export default React.memo(JoinNode);
