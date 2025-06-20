import type { NodeProps } from '@xyflow/react';
import type { OperationNode } from '../types';
import { isSectionBuilder } from '../utils';
import BaseNode from './base-node';
import { SectionIcon } from './icons';

function SectionNode(props: NodeProps<OperationNode<'section'>>) {
  const label = isSectionBuilder(props.data)
    ? props.data.builder
    : props.data.template;
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
