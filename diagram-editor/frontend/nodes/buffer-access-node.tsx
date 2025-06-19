import type { NodeProps } from '@xyflow/react';
import type { DiagramEditorNode } from '../types';
import BaseNode from './base-node';
import { BufferAccessIcon } from './icons';

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
