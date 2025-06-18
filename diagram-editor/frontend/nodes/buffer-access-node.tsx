import type { NodeProps } from '@xyflow/react';
import BaseNode from './base-node';
import { BufferAccessIcon } from './icons';
import type { DiagramEditorNode } from './types';

function BufferAccessNode(props: NodeProps<DiagramEditorNode>) {
  return (
    <BaseNode
      {...props}
      icon={<BufferAccessIcon />}
      label="Buffer Access"
      variant="inputOutput"
    />
  );
}

export default BufferAccessNode;
