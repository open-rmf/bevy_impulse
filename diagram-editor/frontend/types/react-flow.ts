import type {
  Edge as ReactFlowEdge,
  Node as ReactFlowNode,
} from '@xyflow/react';

export type Node<
  D extends Record<string, unknown>,
  T extends string,
> = ReactFlowNode<D, T> & Pick<Required<ReactFlowNode>, 'type' | 'data'>;

export type Edge<
  D extends Record<string, unknown>,
  T extends string,
> = ReactFlowEdge<D, T> & Pick<Required<ReactFlowEdge>, 'type' | 'data'>;
