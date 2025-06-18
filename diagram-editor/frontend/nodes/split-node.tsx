import type { NodeProps } from '@xyflow/react';
import BaseNode from './base-node';
import { SplitIcon } from './icons';
import type { DiagramEditorNode } from './types';

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

export default SplitNode;
