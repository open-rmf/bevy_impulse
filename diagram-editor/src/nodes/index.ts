import { StartNode } from './start-node';
import { TerminateNode } from './terminate-node';

import type { Node } from '@xyflow/react';
import type { DiagramOperation } from '../types/diagram';

export type DiagramEditorNode = Node<
  | DiagramOperation
  // TODO: this is a placeholder for reactflow default nodes, it should be removed when all
  // diagram operations have custom nodes implemented.
  | { type: never; label: string }
  | Record<string, never>
>;

export const NODE_TYPES = {
  start: StartNode,
  terminate: TerminateNode,
};
