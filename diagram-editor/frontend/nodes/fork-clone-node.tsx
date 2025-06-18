import type { NodeProps } from '@xyflow/react';
import BaseNode from './base-node';
import { ForkCloneIcon } from './icons';
import type { DiagramEditorNode } from './types';

function ForkCloneNode(props: NodeProps<DiagramEditorNode>) {
  return (
    <BaseNode
      {...props}
      icon={<ForkCloneIcon />}
      label="Fork Clone"
      variant="inputOutput"
    />
  );
}

export default ForkCloneNode;
