import type { NodeProps } from '@xyflow/react';
import BaseNode from './base-node';
import { ScopeIcon } from './icons';
import type { DiagramEditorNode } from './types';

function ScopeNode(props: NodeProps<DiagramEditorNode>) {
  return (
    <BaseNode
      {...props}
      icon={<ScopeIcon />}
      label="Scope"
      variant="inputOutput"
    />
  );
}

export default ScopeNode;
