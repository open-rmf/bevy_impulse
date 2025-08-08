import type { NodeProps } from '@xyflow/react';
import type { OperationNode } from '.';
import BaseNode, { HandleType } from './base-node';
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
