import type { DiagramEditorNode, OperationNode } from '..';

export function isOperationData(
  data: DiagramEditorNode['data'],
): data is OperationNode['data'] {
  return 'type' in data;
}
