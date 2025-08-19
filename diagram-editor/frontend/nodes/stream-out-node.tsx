import type { NodeProps } from '@xyflow/react';
import { HandleType } from '../handles';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { StreamOutIcon } from './icons';

function StreamOutNodeComp(props: NodeProps<OperationNode<'stream_out'>>) {
  return (
    <BaseNode
      {...props}
      icon={<StreamOutIcon />}
      label="StreamOut"
      variant="input"
      caption={props.data.op.name}
      inputHandleType={HandleType.Stream}
    />
  );
}

export default StreamOutNodeComp;
