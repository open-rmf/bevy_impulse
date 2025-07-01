import type { NodeProps } from '@xyflow/react';
import React from 'react';
import type { DiagramEditorNode } from '../types';
import BaseNode from './base-node';
import { ListenIcon } from './icons';

function ListenNode(props: NodeProps<DiagramEditorNode>) {
  return (
    <BaseNode
      {...props}
      icon={<ListenIcon />}
      label="Listen"
      variant="inputOutput"
    />
  );
}

export default React.memo(ListenNode);
