import type { Node as ReactFlowNode } from '@xyflow/react';
import type { DiagramOperation } from '../types/diagram';

export type BuiltinNodeTypes = 'start' | 'terminate';

export type OperationNodeTypes = DiagramOperation['type'];

export type NodeTypes = BuiltinNodeTypes | OperationNodeTypes;

export type Node<
  D extends Record<string, unknown>,
  T extends NodeTypes,
> = ReactFlowNode<D, T>;

export type BuiltinNode = Node<Record<string, never>, BuiltinNodeTypes>;

export type OperationNode<K extends OperationNodeTypes = OperationNodeTypes> =
  Node<Extract<DiagramOperation, { type: K }>, K>;

export type DiagramEditorNode = BuiltinNode | OperationNode;
