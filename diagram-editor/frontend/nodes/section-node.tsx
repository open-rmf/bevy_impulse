import type { NodeProps } from '@xyflow/react';
import type { OperationNode } from '../types';
import BaseNode from './base-node';
import { SectionIcon } from './icons';

function SectionNode(props: NodeProps<OperationNode<'section'>>) {
  const label = (props.data.builder || props.data.template) as string;
  return (
    <BaseNode
      {...props}
      icon={<SectionIcon />}
      label={label}
      variant="inputOutput"
    />
  );
}

export default SectionNode;
