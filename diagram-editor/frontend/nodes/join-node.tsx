import type { NodeProps } from '@xyflow/react';
import { EdgeCategory } from '../edges';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { JoinIcon } from './icons';

function JoinNodeComp(props: NodeProps<OperationNode<'join'>>) {
  return (
    <BaseNode
      {...props}
      icon={<JoinIcon />}
      label="Join"
      variant="inputOutput"
      inputHandleType={EdgeCategory.Buffer}
    />
  );
}

export default JoinNodeComp;
