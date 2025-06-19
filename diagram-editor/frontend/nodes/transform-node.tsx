import type { NodeProps } from '@xyflow/react';
import type { DiagramEditorNode } from '..';
import BaseNode from './base-node';
import { TransformIcon } from './icons';

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
