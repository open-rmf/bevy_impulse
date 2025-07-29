import type { DiagramOperation } from '../types/api';
import type { Node } from '../types/react-flow';
import BufferAccessNode from './buffer-access-node';
import BufferNode from './buffer-node';
import ForkCloneNode from './fork-clone-node';
import ForkResultNode from './fork-result-node';
import JoinNode from './join-node';
import ListenNode from './listen-node';
import NodeNode from './node-node';
import ScopeNode from './scope-node';
import SectionNode, {
  SectionBufferNode,
  SectionInputNode,
  type SectionInterfaceNode,
  SectionOutputNode,
} from './section-node';
import SerializedJoinNode from './serialized-join-node';
import SplitNode from './split-node';
import StartNode from './start-node';
import StreamOutNode from './stream-out-node';
import TerminateNode from './terminate-node';
import TransformNode from './transform-node';
import UnzipNode from './unzip-node';

export * from './icons';
export type {
  SectionBufferData,
  SectionBufferNode,
  SectionInputData,
  SectionInputNode,
  SectionInterfaceNode,
  SectionInterfaceNodeTypes,
  SectionOutputData,
  SectionOutputNode,
} from './section-node';
export * from './utils';

export const START_ID = '__builtin_start__';
export const TERMINATE_ID = '__builtin_terminate__';

export const NODE_TYPES = {
  start: StartNode,
  terminate: TerminateNode,
  node: NodeNode,
  section: SectionNode,
  sectionInput: SectionInputNode,
  sectionOutput: SectionOutputNode,
  sectionBuffer: SectionBufferNode,
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
};

export type NodeTypes = keyof typeof NODE_TYPES;

export type BuiltinNodeData = { namespace: string };

export type BuiltinNodeTypes = 'start' | 'terminate';

export type BuiltinNode = Node<BuiltinNodeData, BuiltinNodeTypes> & {
  type: BuiltinNodeTypes;
};

export type OperationNodeData<K extends DiagramOperation['type']> = {
  namespace: string;
  opId: string;
  op: DiagramOperation & { type: K };
};

export type OperationNodeTypes = DiagramOperation['type'];

export type OperationNode<K extends OperationNodeTypes = OperationNodeTypes> =
  Node<OperationNodeData<K>, K>;

export type DiagramEditorNode =
  | BuiltinNode
  | OperationNode
  | SectionInterfaceNode;
