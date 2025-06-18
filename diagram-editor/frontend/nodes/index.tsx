import type { DiagramOperation } from '../types/diagram';
import BufferAccessNode from './buffer-access-node';
import BufferNode from './buffer-node';
import ForkCloneNode from './fork-clone-node';
import ForkResultNode from './fork-result-node';
import JoinNode from './join-node';
import ListenNode from './listen-node';
import NodeNode from './node-node';
import ScopeNode from './scope-node';
import SectionNode from './section-node';
import SerializedJoinNode from './serialized-join-node';
import SplitNode from './split-node';
import { StartNode } from './start-node';
import StreamOutNode from './stream-out-node';
import { TerminateNode } from './terminate-node';
import TransformNode from './transform-node';
import type { DiagramEditorNode, NodeTypes, OperationNode } from './types';
import UnzipNode from './unzip-node';

export type * from './types';

export const START_ID = 'builtin:start';
export const TERMINATE_ID = 'builtin:terminate';

export const NODE_TYPES = {
  start: StartNode,
  terminate: TerminateNode,
  node: NodeNode,
  section: SectionNode,
  fork_clone: ForkCloneNode,
  unzip: UnzipNode,
  fork_result: ForkResultNode,
  split: SplitNode,
  join: JoinNode,
  serialized_join: SerializedJoinNode,
  transform: TransformNode,
  buffer: BufferNode,
  buffer_access: BufferAccessNode,
  listen: ListenNode,
  scope: ScopeNode,
  stream_out: StreamOutNode,
} satisfies Record<NodeTypes, unknown>;

export function isOperationNode(
  node: DiagramEditorNode,
): node is OperationNode {
  return !node.id.startsWith('builtin:');
}

export function extractOperation(node: OperationNode): DiagramOperation {
  const op: DiagramOperation = { ...node.data };
  const opIdKey = 'opId';
  delete op[opIdKey];
  return op;
}
