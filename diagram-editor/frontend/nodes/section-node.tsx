import { type NodeProps, Position } from '@xyflow/react';
import { Handle, HandleType } from '../handles';
import { useRegistry } from '../registry-provider';
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

export function SectionNodeComp(props: NodeProps<OperationNode<'section'>>) {
  const registry = useRegistry();
  const label = (() => {
    if (props.data.op.display_text) {
      return props.data.op.display_text;
    }
    if (isSectionBuilder(props.data.op)) {
      const builderMetadata = registry.sections[props.data.op.builder];
      if (builderMetadata) {
        return builderMetadata.default_display_text;
      }
    } else {
      if (props.data.op.template) {
        return 'Template';
      }
    }
    return 'Select Section';
  })();

  const caption = isSectionBuilder(props.data.op)
    ? props.data.op.builder
    : props.data.op.template;

  return (
    <BaseNode
      {...props}
      icon={<SectionIcon />}
      label={label}
      caption={caption}
      handles={
        <>
          <Handle
            type="target"
            position={Position.Top}
            isConnectable={props.isConnectable}
            variant={HandleType.DataBuffer}
          />
          <Handle
            type="source"
            position={Position.Bottom}
            isConnectable={props.isConnectable}
            variant={HandleType.Data}
          />
        </>
      }
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
      caption={props.data.remappedId}
      handles={
        <Handle
          type="source"
          position={Position.Bottom}
          isConnectable={props.isConnectable}
          variant={HandleType.Data}
        />
      }
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
      caption={props.data.outputId}
      handles={
        <Handle
          type="target"
          position={Position.Top}
          isConnectable={props.isConnectable}
          variant={HandleType.Data}
        />
      }
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
      caption={props.data.remappedId}
      handles={
        <Handle
          type="source"
          position={Position.Bottom}
          isConnectable={props.isConnectable}
          variant={HandleType.Buffer}
        />
      }
    />
  );
}
