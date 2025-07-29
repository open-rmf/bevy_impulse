import type { NodeProps } from '@xyflow/react';
import type { NextOperation } from '../types/api';
import type { Node } from '../types/react-flow';
import { isSectionBuilder } from '../utils/operation';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import {
  SectionBufferIcon,
  SectionIcon,
  SectionInputIcon,
  SectionOutputIcon,
} from './icons';

export type SectionInputData = {
  remappedId: string;
  targetId: NextOperation;
};

export type SectionInputNode = Node<SectionInputData, 'sectionInput'>;

export type SectionOutputData = {
  outputId: string;
};

export type SectionOutputNode = Node<SectionOutputData, 'sectionOutput'>;

export type SectionBufferData = SectionInputData;

export type SectionBufferNode = Node<SectionBufferData, 'sectionBuffer'>;

export type SectionInterfaceNode =
  | SectionInputNode
  | SectionOutputNode
  | SectionBufferNode;

export function SectionNode(props: NodeProps<OperationNode<'section'>>) {
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

export default SectionNode;

export type SectionInterfaceNodeTypes =
  | 'sectionInput'
  | 'sectionOutput'
  | 'sectionBuffer';

export function SectionInputNode(props: NodeProps<SectionInputNode>) {
  return (
    <BaseNode
      {...props}
      color="secondary"
      icon={<SectionInputIcon />}
      label={props.data.remappedId}
      variant="output"
    />
  );
}

export function SectionOutputNode(props: NodeProps<SectionOutputNode>) {
  return (
    <BaseNode
      {...props}
      color="secondary"
      icon={<SectionOutputIcon />}
      label="Output"
      variant="input"
    />
  );
}

export function SectionBufferNode(props: NodeProps<SectionBufferNode>) {
  return (
    <BaseNode
      {...props}
      color="secondary"
      icon={<SectionBufferIcon />}
      label={props.data.remappedId}
      variant="output"
    />
  );
}
