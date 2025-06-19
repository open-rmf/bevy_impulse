import type { NodeProps } from '@xyflow/react';
import type { DiagramEditorNode } from '..';
import BaseNode from './base-node';
import { ForkCloneIcon } from './icons';

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
