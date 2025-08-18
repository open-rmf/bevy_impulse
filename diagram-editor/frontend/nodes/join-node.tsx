import type { NodeProps } from '@xyflow/react';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { HandleType } from '../handles';
import { JoinIcon } from './icons';

function JoinNodeComp(props: NodeProps<OperationNode<'join'>>) {
  return (
    <BaseNode
      {...props}
      icon={<JoinIcon />}
      label="Join"
      variant="inputOutput"
      inputHandleType={HandleType.Buffer}
    />
  );
}

export default JoinNodeComp;
