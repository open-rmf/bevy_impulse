import type { NodeProps } from '@xyflow/react';
import type { DiagramEditorNode } from '..';
import BaseNode from './base-node';
import { ScopeIcon } from './icons';

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
