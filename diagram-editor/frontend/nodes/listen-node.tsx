import type { NodeProps } from '@xyflow/react';
import { HandleType } from '../handles';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { ListenIcon } from './icons';

function ListenNodeComp(props: NodeProps<OperationNode<'listen'>>) {
  return (
    <BaseNode
      {...props}
      icon={<ListenIcon />}
      label="Listen"
      variant="inputOutput"
      inputHandleType={HandleType.Buffer}
    />
  );
}

export default ListenNodeComp;
