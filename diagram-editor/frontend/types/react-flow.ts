import type {
  Edge as ReactFlowEdge,
  Node as ReactFlowNode,
} from '@xyflow/react';
import type { HandleId } from '../handles';

export type Node<
  D extends Record<string, unknown>,
  T extends string,
> = ReactFlowNode<D, T> & Pick<Required<ReactFlowNode>, 'type' | 'data'>;

export type Edge<
  O extends Record<string, unknown>,
  I extends { type: string },
  T extends string,
> = ReactFlowEdge<{ output: O; input: I }, T> &
  Pick<Required<ReactFlowEdge>, 'type' | 'data'> & {
    sourceHandle?: HandleId;
    targetHandle?: HandleId;
  };
