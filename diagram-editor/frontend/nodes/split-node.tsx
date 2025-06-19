import type { NodeProps } from '@xyflow/react';
import type { DiagramEditorNode } from '..';
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

export default SplitNode;
