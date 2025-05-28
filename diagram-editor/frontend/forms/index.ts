import type { NodeReplaceChange } from '@xyflow/react';
import type { DiagramEditorNode } from '../nodes';

export interface OperationFormProps {
  node: DiagramEditorNode;
  onChange?: (change: NodeReplaceChange<DiagramEditorNode>) => void;
}
