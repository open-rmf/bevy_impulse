import type { NodeProps } from '@xyflow/react';
import React from 'react';
import type { DiagramEditorNode } from '../types';
import BaseNode from './base-node';
import { BufferIcon } from './icons';

function BufferNode(props: NodeProps<DiagramEditorNode>) {
  return (
    <BaseNode
      {...props}
      icon={<BufferIcon />}
      label="Buffer"
      variant="inputOutput"
    />
  );
}

export default React.memo(BufferNode);
