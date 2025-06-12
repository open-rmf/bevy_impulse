import type { EdgeReplaceChange, NodeReplaceChange } from '@xyflow/react';
import type {
  DiagramEditorEdge,
  ForkResultErrEdge,
  ForkResultOkEdge,
  UnzipEdge,
} from '../edges';
import type { DiagramEditorNode, OperationNode } from '../nodes';
import { exhaustiveCheck } from '../utils/exhaustive-check';
import BufferEdgeForm, { type BufferEdge } from './buffer-edge-form';
import BufferForm from './buffer-form';
import ForkResultEdgeForm from './fork-result-edge-form';
import NodeForm from './node-form';
import SplitEdgeForm, { type SplitEdge } from './split-edge-form';
import UnzipEdgeForm from './unzip-edge-form';

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
    case 'buffer_access':
    case 'fork_clone':
    case 'fork_result':
    case 'join':
    case 'listen':
    case 'section':
    case 'serialized_join':
    case 'split':
    case 'transform':
    case 'unzip': {
      return false;
    }
    default: {
      exhaustiveCheck(node.data);
      throw new Error('unknown node');
    }
  }
}

export interface EditEdgeFormProps {
  edge: DiagramEditorEdge;
  onChange?: (change: EdgeReplaceChange<DiagramEditorEdge>) => void;
}

export function EditEdgeForm({ edge, onChange }: EditEdgeFormProps) {
  switch (edge.type) {
    case 'bufferKey':
    case 'bufferSeq': {
      return (
        <BufferEdgeForm
          edge={edge as BufferEdge}
          onChange={(change) => onChange?.(change)}
        />
      );
    }
    case 'forkResultOk':
    case 'forkResultErr': {
      return (
        <ForkResultEdgeForm
          edge={edge as ForkResultOkEdge | ForkResultErrEdge}
          onChange={(change) => onChange?.(change)}
        />
      );
    }
    case 'splitKey':
    case 'splitRemaining':
    case 'splitSeq': {
      return (
        <SplitEdgeForm
          edge={edge as SplitEdge}
          onChange={(change) => onChange?.(change)}
        />
      );
    }
    case 'unzip': {
      return (
        <UnzipEdgeForm
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
  switch (edge.type) {
    case 'default': {
      return false;
    }
    case 'bufferKey':
    case 'bufferSeq':
    case 'forkResultErr':
    case 'forkResultOk':
    case 'splitKey':
    case 'splitRemaining':
    case 'splitSeq':
    case 'unzip': {
      return true;
    }
    default: {
      exhaustiveCheck(edge);
      throw new Error('unknown edge');
    }
  }
}
