import type { NodeProps } from '@xyflow/react';
import React from 'react';
import type { OperationNode } from '../types';
import { isSectionBuilder } from '../utils';
import BaseNode from './base-node';
import { SectionIcon } from './icons';

function SectionNode(props: NodeProps<OperationNode<'section'>>) {
  const label = isSectionBuilder(props.data.op)
    ? props.data.op.builder
    : props.data.op.template;
  return (
    <BaseNode
      {...props}
      icon={<SectionIcon />}
      label={label}
      variant="inputOutput"
    />
  );
}

export default React.memo(SectionNode);
