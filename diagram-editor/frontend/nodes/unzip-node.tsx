import type { NodeProps } from '@xyflow/react';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { UnzipIcon } from './icons';

function UnzipNode(props: NodeProps<OperationNode<'unzip'>>) {
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
