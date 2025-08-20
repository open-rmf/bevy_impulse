import type { XYPosition } from '@xyflow/react';
import { v4 as uuidv4 } from 'uuid';
import type { DiagramOperation, NextOperation } from '../types/api';
import { calculateScopeBounds, LAYOUT_OPTIONS } from '../utils/layout';
import { joinNamespaces } from '../utils/namespace';
import {
  type DiagramEditorNode,
  type OperationNode,
  type SectionBufferNode,
  type SectionInputNode,
  type SectionOutputNode,
  START_ID,
  TERMINATE_ID,
} from '.';

export function createStartNode(
  namespace: string,
  position: XYPosition,
): DiagramEditorNode {
  return {
    // NOTE: atm `NodeManager` relies on how the id of builtin nodes is computed to locate them.
    id: joinNamespaces(namespace, START_ID),
    type: 'start',
    position,
    selectable: false,
    data: { namespace },
  };
}

export function createTerminateNode(
  namespace: string,
  position: XYPosition,
): DiagramEditorNode {
  return {
    // NOTE: atm `NodeManager` relies on how the id of builtin nodes is computed to locate them.
    id: joinNamespaces(namespace, TERMINATE_ID),
    type: 'terminate',
    position,
    selectable: false,
    data: { namespace },
  };
}

/**
 * Create a section input node. For simplicity, remapping the input is not supported, `targetId`
 * must point to the id of an operation in the template.
 */
export function createSectionInputNode(
  remappedId: string,
  targetId: NextOperation,
  position: XYPosition,
): SectionInputNode {
  return {
    id: uuidv4(),
    type: 'sectionInput',
    data: { remappedId, targetId },
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
  remappedId: string,
  targetId: NextOperation,
  position: XYPosition,
): SectionBufferNode {
  return {
    id: uuidv4(),
    type: 'sectionBuffer',
    data: { remappedId, targetId },
    position,
  };
}

export function createOperationNode(
  namespace: string,
  parentId: string | undefined,
  position: XYPosition,
  op: Exclude<DiagramOperation, { type: 'scope' }>,
  opId: string,
): OperationNode {
  return {
    id: uuidv4(),
    type: op.type,
    position,
    data: {
      namespace,
      opId,
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
  opId: string,
): [OperationNode<'scope'>, ...DiagramEditorNode[]] {
  const scopeId = uuidv4();
  const children: DiagramEditorNode[] = [
    {
      id: joinNamespaces(namespace, opId, START_ID),
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
      id: joinNamespaces(namespace, opId, TERMINATE_ID),
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
        opId,
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
