import type { NodeProps } from '@xyflow/react';
import type { Node } from '../types/react-flow';
import { isSectionBuilder, type ROOT_NAMESPACE } from '../utils';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import {
  SectionBufferIcon,
  SectionIcon,
  SectionInputIcon,
  SectionOutputIcon,
} from './icons';

export type SectionInterfaceData = {
  // section interfaces is always in the root namespace
  namespace: typeof ROOT_NAMESPACE;
  connectKey: string;
};

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

export type SectionInterfaceNode<
  K extends SectionInterfaceNodeTypes = SectionInterfaceNodeTypes,
> = Node<SectionInterfaceData, K>;

export function SectionInputNode(
  props: NodeProps<SectionInterfaceNode<'sectionInput'>>,
) {
  return (
    <BaseNode
      {...props}
      color="secondary"
      icon={<SectionInputIcon />}
      label={props.data.connectKey}
      variant="output"
    />
  );
}

export function SectionOutputNode(
  props: NodeProps<SectionInterfaceNode<'sectionOutput'>>,
) {
  return (
    <BaseNode
      {...props}
      color="secondary"
      icon={<SectionOutputIcon />}
      label={props.data.connectKey}
      variant="input"
    />
  );
}

export function SectionBufferNode(
  props: NodeProps<SectionInterfaceNode<'sectionBuffer'>>,
) {
  return (
    <BaseNode
      {...props}
      color="secondary"
      icon={<SectionBufferIcon />}
      label={props.data.connectKey}
      variant="output"
    />
  );
}
