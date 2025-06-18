import type { NodeProps } from '@xyflow/react';
import BaseNode from './base-node';
import { TransformIcon } from './icons';
import type { DiagramEditorNode } from './types';

function TransformNode(props: NodeProps<DiagramEditorNode>) {
  return (
    <BaseNode
      {...props}
      icon={<TransformIcon />}
      label="Transform"
      variant="inputOutput"
    />
  );
}

export default TransformNode;
