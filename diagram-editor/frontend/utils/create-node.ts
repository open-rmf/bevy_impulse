import type { XYPosition } from '@xyflow/react';
import { v4 as uuidv4 } from 'uuid';
import type {
  DiagramEditorNode,
  OperationNode,
  SectionBufferNode,
  SectionInputNode,
  SectionOutputNode,
} from '../nodes';
import type { DiagramOperation } from '../types/api';
import { calculateScopeBounds, LAYOUT_OPTIONS } from './layout';
import { joinNamespaces } from './namespace';

/**
 * Create a section input node. For simplicity, remapping the input is not supported, `targetId`
 * must point to the id of an operation in the template.
 */
export function createSectionInputNode(
  targetId: string,
  position: XYPosition,
): SectionInputNode {
  return {
    id: uuidv4(),
    type: 'sectionInput',
    data: { remappedId: targetId, targetId },
    position,
  };
}

export function createSectionOutputNode(
  outputId: string,
  position: XYPosition,
): SectionOutputNode {
  return {
    id: uuidv4(),
    type: 'sectionOutput',
    data: { outputId },
    position,
  };
}

/**
 * Create a section input node. For simplicity, remapping the input is not supported, `targetId`
 * must point to the id of a `buffer` operation in the template.
 */
export function createSectionBufferNode(
  targetId: string,
  position: XYPosition,
): SectionBufferNode {
  return {
    id: uuidv4(),
    type: 'sectionBuffer',
    data: { remappedId: targetId, targetId },
    position,
  };
}

export function createOperationNode(
  namespace: string,
  parentId: string | undefined,
  position: XYPosition,
  op: Exclude<DiagramOperation, { type: 'scope' }>,
): OperationNode {
  return {
    id: uuidv4(),
    type: op.type,
    position,
    data: {
      namespace,
      opId: uuidv4(),
      op,
    },
    ...(parentId && { parentId }),
  };
}

export function createScopeNode(
  namespace: string,
  parentId: string | undefined,
  position: XYPosition,
  op: DiagramOperation & { type: 'scope' },
): DiagramEditorNode[] {
  const scopeId = uuidv4();
  const children: DiagramEditorNode[] = [
    {
      id: uuidv4(),
      type: 'start',
      position: {
        x: LAYOUT_OPTIONS.scopePadding.leftRight,
        y: LAYOUT_OPTIONS.scopePadding.topBottom,
      },
      data: {
        namespace: joinNamespaces(namespace, scopeId),
      },
      parentId: scopeId,
    },
    {
      id: uuidv4(),
      type: 'terminate',
      position: {
        x: LAYOUT_OPTIONS.scopePadding.leftRight,
        y: LAYOUT_OPTIONS.scopePadding.topBottom * 5,
      },
      data: {
        namespace: joinNamespaces(namespace, scopeId),
      },
      parentId: scopeId,
    },
  ];
  const scopeBounds = calculateScopeBounds(
    children.map((child) => child.position),
  );
  return [
    {
      id: scopeId,
      type: 'scope',
      position: {
        x: position.x + scopeBounds.x,
        y: position.y + scopeBounds.y,
      },
      data: {
        namespace,
        opId: uuidv4(),
        op,
      },
      width: scopeBounds.width,
      height: scopeBounds.height,
      zIndex: -1,
      ...(parentId && { parentId }),
    },
    ...children,
  ];
}
