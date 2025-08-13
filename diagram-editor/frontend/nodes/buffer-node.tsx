import type { NodeProps } from '@xyflow/react';
import type { OperationNode } from '.';
import BaseNode, { HandleType } from './base-node';
import { BufferIcon } from './icons';

function BufferNodeComp(props: NodeProps<OperationNode<'buffer'>>) {
  return (
    <BaseNode
      {...props}
      icon={<BufferIcon />}
      label="Buffer"
      variant="inputOutput"
      outputHandleType={HandleType.Buffer}
    />
  );
}

export default BufferNodeComp;
