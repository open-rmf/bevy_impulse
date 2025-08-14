import { useReactFlow as _useReactFlow } from '@xyflow/react';
import type { DiagramEditorEdge } from './edges';
import type { DiagramEditorNode } from './nodes';

export function useReactFlow() {
  return _useReactFlow<DiagramEditorNode, DiagramEditorEdge>();
}
