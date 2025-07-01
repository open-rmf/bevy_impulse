import type { NodeProps } from '@xyflow/react';
import React from 'react';
import type { DiagramEditorNode } from '../types';
import BaseNode from './base-node';
import { ForkResultIcon } from './icons';

function ForkResultNode(props: NodeProps<DiagramEditorNode>) {
  return (
    <BaseNode
      {...props}
      icon={<ForkResultIcon />}
      label="Fork Result"
      variant="inputOutput"
    />
  );
}

export default React.memo(ForkResultNode);
