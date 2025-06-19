import type { NodeProps } from '@xyflow/react';
import type { DiagramEditorNode } from '..';
import BaseNode from './base-node';
import { StreamOutIcon } from './icons';

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
