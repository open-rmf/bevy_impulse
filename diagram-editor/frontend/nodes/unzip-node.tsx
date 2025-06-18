import type { NodeProps } from '@xyflow/react';
import BaseNode from './base-node';
import { UnzipIcon } from './icons';
import type { DiagramEditorNode } from './types';

function UnzipNode(props: NodeProps<DiagramEditorNode>) {
  return (
    <BaseNode
      {...props}
      icon={<UnzipIcon />}
      label="Unzip"
      variant="inputOutput"
    />
  );
}

export default UnzipNode;
