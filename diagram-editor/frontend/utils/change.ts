import type { NodeChange, NodeRemoveChange } from '@xyflow/react';
import type { DiagramEditorNode } from '../nodes';

export function isRemoveChange(
  change: NodeChange<DiagramEditorNode>,
): change is NodeRemoveChange {
  return change.type === 'remove';
}
