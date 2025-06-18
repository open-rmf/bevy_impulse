import type { DiagramEditorNode, OperationNode } from './types';

export function isOperationData(
  data: DiagramEditorNode['data'],
): data is OperationNode['data'] {
  return 'type' in data;
}
