import type { NodeProps } from '@xyflow/react';
import type { NextOperation } from '../types/api';
import type { Node } from '../types/react-flow';
import { isSectionBuilder } from '../utils/operation';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { HandleType } from '../handles';
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

export function SectionNodeComp(props: NodeProps<OperationNode<'section'>>) {
  const label = isSectionBuilder(props.data.op)
    ? props.data.op.builder
    : props.data.op.template;
  return (
    <BaseNode
      {...props}
      icon={<SectionIcon />}
      label={label}
      variant="inputOutput"
      inputHandleType={HandleType.DataBuffer}
    />
  );
}

export default SectionNodeComp;

export type SectionInterfaceNodeTypes =
  | 'sectionInput'
  | 'sectionOutput'
  | 'sectionBuffer';

export function SectionInputNodeComp(props: NodeProps<SectionInputNode>) {
  return (
    <BaseNode
      {...props}
      color="secondary"
      icon={<SectionInputIcon />}
      label="Section Input"
      variant="output"
      caption={props.data.remappedId}
    />
  );
}

export function SectionOutputNodeComp(props: NodeProps<SectionOutputNode>) {
  return (
    <BaseNode
      {...props}
      color="secondary"
      icon={<SectionOutputIcon />}
      label="Section Output"
      variant="input"
      caption={props.data.outputId}
    />
  );
}

export function SectionBufferNodeComp(props: NodeProps<SectionBufferNode>) {
  return (
    <BaseNode
      {...props}
      color="secondary"
      icon={<SectionBufferIcon />}
      label="Section Buffer"
      variant="output"
      caption={props.data.remappedId}
      outputHandleType={HandleType.Buffer}
    />
  );
}
