import type { NodeProps } from '@xyflow/react';
import BaseNode from './base-node';
import { StreamOutIcon } from './icons';
import type { DiagramEditorNode } from './types';

function StreamOutNode(props: NodeProps<DiagramEditorNode>) {
  return (
    <BaseNode
      {...props}
      icon={<StreamOutIcon />}
      label="StreamOut"
      variant="input"
    />
  );
}

export default StreamOutNode;
