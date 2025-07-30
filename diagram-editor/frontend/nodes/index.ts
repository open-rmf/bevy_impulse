import type { DiagramOperation } from '../types/api';
import type { Node } from '../types/react-flow';
import BufferAccessNodeComp from './buffer-access-node';
import BufferNodeComp from './buffer-node';
import ForkCloneNodeComp from './fork-clone-node';
import ForkResultNodeComp from './fork-result-node';
import JoinNodeComp from './join-node';
import ListenNodeComp from './listen-node';
import NodeNodeComp from './node-node';
import ScopeNodeComp from './scope-node';
import SectionNodeComp, {
  SectionBufferNodeComp,
  SectionInputNodeComp,
  type SectionInterfaceNode,
  SectionOutputNodeComp,
} from './section-node';
import SerializedJoinNodeComp from './serialized-join-node';
import SplitNodeComp from './split-node';
import StartNodeComp from './start-node';
import StreamOutNodeComp from './stream-out-node';
import TerminateNodeComp from './terminate-node';
import TransformNodeComp from './transform-node';
import UnzipNodeComp from './unzip-node';

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
  start: StartNodeComp,
  terminate: TerminateNodeComp,
  node: NodeNodeComp,
  section: SectionNodeComp,
  sectionInput: SectionInputNodeComp,
  sectionOutput: SectionOutputNodeComp,
  sectionBuffer: SectionBufferNodeComp,
  fork_clone: ForkCloneNodeComp,
  unzip: UnzipNodeComp,
  fork_result: ForkResultNodeComp,
  split: SplitNodeComp,
  join: JoinNodeComp,
  serialized_join: SerializedJoinNodeComp,
  transform: TransformNodeComp,
  buffer: BufferNodeComp,
  buffer_access: BufferAccessNodeComp,
  listen: ListenNodeComp,
  scope: ScopeNodeComp,
  stream_out: StreamOutNodeComp,
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
