import type { NodeProps } from '@xyflow/react';
import { EdgeCategory } from '../edges';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { BufferIcon } from './icons';

function BufferNodeComp(props: NodeProps<OperationNode<'buffer'>>) {
  return (
    <BaseNode
      {...props}
      icon={<BufferIcon />}
      label="Buffer"
      variant="inputOutput"
      outputHandleType={EdgeCategory.Buffer}
    />
  );
}

export default BufferNodeComp;
