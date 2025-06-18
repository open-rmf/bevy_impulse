import type { NodeProps } from '@xyflow/react';
import BaseNode from './base-node';
import { BufferIcon } from './icons';
import type { DiagramEditorNode } from './types';

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

export default BufferNode;
