import type { NodeProps } from '@xyflow/react';
import type { DiagramEditorNode } from '..';
import BaseNode from './base-node';
import { ForkResultIcon } from './icons';

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
