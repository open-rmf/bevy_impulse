import type { NodeProps } from '@xyflow/react';
import BaseNode from './base-node';
import { ForkResultIcon } from './icons';
import type { DiagramEditorNode } from './types';

function ForkResultNode(props: NodeProps<DiagramEditorNode>) {
  return (
    <BaseNode
      {...props}
      icon={<ForkResultIcon />}
      label="Fork Result"
      variant="inputOutput"
    />
  );
}

export default ForkResultNode;
