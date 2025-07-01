import type { NodeProps } from '@xyflow/react';
import React from 'react';
import type { DiagramEditorNode } from '../types';
import BaseNode from './base-node';
import { SplitIcon } from './icons';

function SplitNode(props: NodeProps<DiagramEditorNode>) {
  return (
    <BaseNode
      {...props}
      icon={<SplitIcon />}
      label="Split"
      variant="inputOutput"
    />
  );
}

export default React.memo(SplitNode);
