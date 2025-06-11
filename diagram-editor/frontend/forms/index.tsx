import type { NodeReplaceChange } from '@xyflow/react';
import type { DiagramEditorNode, OperationNode } from '../nodes';
import NodeForm from './node-form';
import BufferForm from './buffer-form';

export interface OperationFormProps {
  node: DiagramEditorNode;
  onChange?: (change: NodeReplaceChange<DiagramEditorNode>) => void;
}

function OperationForm({ node, onChange }: OperationFormProps) {
  switch (node.data.type) {
    case 'node': {
      return (
        <NodeForm
          node={node as OperationNode<'node'>}
          onChange={(change) => onChange?.(change)}
        />
      );
    }
    case 'buffer': {
      return (
        <BufferForm
          node={node as OperationNode<'buffer'>}
          onChange={(change) => onChange?.(change)}
        />
      );
    }
    case 'fork_clone':
    case 'unzip':
    case 'fork_result':
    case 'split':
    case 'join':
    case 'serialized_join': {
      return null;
    }
    default: {
      throw new Error('not implemented');
    }
  }
}

export function hasOperationForm(node: DiagramEditorNode): boolean {
  switch (node.data.type) {
    case 'node':
    case 'buffer': {
      return true;
    }
    default: {
      return false;
    }
  }
}

export default OperationForm;
