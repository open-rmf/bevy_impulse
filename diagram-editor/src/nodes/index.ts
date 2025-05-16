import { StartNode } from './start-node';
import { TerminateNode } from './terminate-node';

import type { Node } from '@xyflow/react';
import type { DiagramOperation } from '../types/diagram';

export type DiagramEditorNode = Node<DiagramOperation | Record<string, never>>;

export const NODE_TYPES = {
  start: StartNode,
  terminate: TerminateNode,
};
