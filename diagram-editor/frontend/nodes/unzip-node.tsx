import type { NodeProps } from '@xyflow/react';
import type { DiagramEditorNode } from '..';
import BaseNode from './base-node';
import { UnzipIcon } from './icons';

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
