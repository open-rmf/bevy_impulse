import type { NodeProps } from '@xyflow/react';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { HandleType } from '../handles';
import { BufferAccessIcon } from './icons';

function BufferAccessNodeComp(
  props: NodeProps<OperationNode<'buffer_access'>>,
) {
  return (
    <BaseNode
      {...props}
      icon={<BufferAccessIcon />}
      label="Buffer Access"
      variant="inputOutput"
      inputHandleType={HandleType.DataBuffer}
    />
  );
}

export default BufferAccessNodeComp;
