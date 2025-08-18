import type { NodeChange, NodeRemoveChange } from '@xyflow/react';
import {
  type DiagramEditorNode,
  isOperationNode,
  isSectionInterfaceNode,
  type OperationNode,
} from '../nodes';
import { exhaustiveCheck } from '../utils/exhaustive-check';
import BaseEditOperationForm from './base-edit-operation-form';
import BufferForm, { type BufferFormProps } from './buffer-form';
import EditScopeForm, { type ScopeFormProps } from './edit-scope-form';
import NodeForm, { type NodeFormProps } from './node-form';
import {
  SectionBufferForm,
  SectionForm,
  type SectionFormProps,
  SectionInputForm,
  SectionOutputForm,
} from './section-form';
import { StreamOutForm, type StreamOutFormProps } from './stream-out-form';
import TransformForm, { type TransformFormProps } from './transform-form';

interface EditOperationNodeFormProps {
  node: OperationNode;
  onChange?: (change: NodeChange<DiagramEditorNode>) => void;
  onDelete?: (change: NodeRemoveChange) => void;
}

function EditOperationNodeForm(props: EditOperationNodeFormProps) {
  switch (props.node.data.op.type) {
    case 'node': {
      return <NodeForm {...(props as NodeFormProps)} />;
    }
    case 'buffer': {
      return <BufferForm {...(props as BufferFormProps)} />;
    }
    case 'scope': {
      return <EditScopeForm {...(props as ScopeFormProps)} />;
    }
    case 'section': {
      return <SectionForm {...(props as SectionFormProps)} />;
    }
    case 'transform': {
      return <TransformForm {...(props as TransformFormProps)} />;
    }
    case 'stream_out': {
      return <StreamOutForm {...(props as StreamOutFormProps)} />;
    }
    default: {
      return <BaseEditOperationForm {...props} />;
    }
  }
}

interface EditNodeFormProps {
  node: DiagramEditorNode;
  onChange?: (change: NodeChange<DiagramEditorNode>) => void;
  onDelete?: (change: NodeRemoveChange) => void;
}

function EditNodeForm(props: EditNodeFormProps) {
  if (isOperationNode(props.node)) {
    return <EditOperationNodeForm {...props} node={props.node} />;
  } else if (isSectionInterfaceNode(props.node)) {
    switch (props.node.type) {
      case 'sectionInput':
        return <SectionInputForm {...props} node={props.node} />;
      case 'sectionOutput':
        return <SectionOutputForm {...props} node={props.node} />;
      case 'sectionBuffer':
        return <SectionBufferForm {...props} node={props.node} />;
      default:
        exhaustiveCheck(props.node);
        throw new Error('unknown node type');
    }
  } else {
    return null;
  }
}

export default EditNodeForm;
