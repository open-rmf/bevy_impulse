import type { NodeChange, NodeRemoveChange } from '@xyflow/react';
import type { DiagramEditorNode } from '../nodes';
import BufferForm from './buffer-form';
import EditOperationForm, {
  type EditOperationFormProps,
} from './edit-operation-form';
import EditScopeForm from './edit-scope-form';
import NodeForm from './node-form';
import TransformForm from './transform-form';

export interface EditNodeFormProps {
  node: DiagramEditorNode;
  onChange?: (change: NodeChange<DiagramEditorNode>) => void;
  onDelete?: (change: NodeRemoveChange) => void;
}

function EditNodeForm(props: EditOperationFormProps) {
  switch (props.node.data.op.type) {
    case 'node': {
      return <NodeForm {...(props as EditOperationFormProps<'node'>)} />;
    }
    case 'buffer': {
      return <BufferForm {...(props as EditOperationFormProps<'buffer'>)} />;
    }
    case 'scope': {
      return <EditScopeForm {...(props as EditOperationFormProps<'scope'>)} />;
    }
    case 'transform': {
      return (
        <TransformForm {...(props as EditOperationFormProps<'transform'>)} />
      );
    }
    default: {
      return <EditOperationForm {...props} />;
    }
  }
}

export default EditNodeForm;
