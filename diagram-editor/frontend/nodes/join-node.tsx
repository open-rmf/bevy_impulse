import type { NodeProps } from '@xyflow/react';
import BaseNode from './base-node';
import { JoinIcon } from './icons';
import type { DiagramEditorNode } from './types';

function JoinNode(props: NodeProps<DiagramEditorNode>) {
  return (
    <BaseNode
      {...props}
      icon={<JoinIcon />}
      label="Join"
      variant="inputOutput"
    />
  );
}

export default JoinNode;
