import type { EdgeReplaceChange, NodeReplaceChange } from '@xyflow/react';
import {
  type DiagramEditorEdge,
  type DiagramEditorNode,
  EdgeType,
  type OperationNode,
  type UnzipEdge,
} from '../nodes';
import BufferForm from './buffer-form';
import NodeForm from './node-form';
import UnzipForm from './unzip-edge-form';

export interface EditNodeFormProps {
  node: DiagramEditorNode;
  onChange?: (change: NodeReplaceChange<DiagramEditorNode>) => void;
}

export function EditNodeForm({ node, onChange }: EditNodeFormProps) {
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
    default: {
      return null;
    }
  }
}

export function nodeHasEditForm(node: DiagramEditorNode): boolean {
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

export interface EditEdgeFormProps {
  edge: DiagramEditorEdge;
  onChange?: (change: EdgeReplaceChange<DiagramEditorEdge>) => void;
}

export function EditEdgeForm({ edge, onChange }: EditEdgeFormProps) {
  switch (edge.data?.type) {
    case EdgeType.Unzip: {
      return (
        <UnzipForm
          edge={edge as UnzipEdge}
          onChange={(change) => onChange?.(change)}
        />
      );
    }
    default: {
      return null;
    }
  }
}

export function edgeHasEditForm(edge: DiagramEditorEdge): boolean {
  switch (edge.data?.type) {
    case EdgeType.Basic: {
      return false;
    }
    default: {
      return true;
    }
  }
}
