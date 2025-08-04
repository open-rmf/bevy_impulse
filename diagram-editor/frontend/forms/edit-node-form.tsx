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
  EditSectionBufferForm,
  EditSectionInputForm,
  EditSectionOutputForm,
} from './section-form';
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
    case 'transform': {
      return <TransformForm {...(props as TransformFormProps)} />;
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
        return <EditSectionInputForm {...props} node={props.node} />;
      case 'sectionOutput':
        return <EditSectionOutputForm {...props} node={props.node} />;
      case 'sectionBuffer':
        return <EditSectionBufferForm {...props} node={props.node} />;
      default:
        exhaustiveCheck(props.node);
        throw new Error('unknown node type');
    }
  } else {
    return null;
  }
}

export default EditNodeForm;
