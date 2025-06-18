import type { NodeReplaceChange } from '@xyflow/react';

import type { DiagramEditorNode, OperationNode } from '../nodes';
import { exhaustiveCheck } from '../utils/exhaustive-check';
import BufferForm from './buffer-form';
import NodeForm from './node-form';
import TransformForm from './transform-form';

export interface EditNodeFormProps {
  node: DiagramEditorNode;
  onChange?: (change: NodeReplaceChange<DiagramEditorNode>) => void;
}

function EditNodeForm({ node, onChange }: EditNodeFormProps) {
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
    case 'transform': {
      return (
        <TransformForm
          node={node as OperationNode<'transform'>}
          onChange={(change) => onChange?.(change)}
        />
      );
    }
    default: {
      return null;
    }
  }
}

export default EditNodeForm;

export function nodeHasEditForm(node: DiagramEditorNode): boolean {
  switch (node.data.type) {
    case 'node':
    case 'buffer':
    case 'transform': {
      return true;
    }
    case 'buffer_access':
    case 'fork_clone':
    case 'fork_result':
    case 'join':
    case 'listen':
    case 'section':
    case 'serialized_join':
    case 'split':
    case 'unzip':
    case 'scope':
    case 'stream_out': {
      return false;
    }
    default: {
      exhaustiveCheck(node.data);
      throw new Error('unknown node');
    }
  }
}
