import type { NodeProps } from '@xyflow/react';
import React from 'react';
import type { DiagramEditorNode } from '../types';
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

export default React.memo(ScopeNode);
