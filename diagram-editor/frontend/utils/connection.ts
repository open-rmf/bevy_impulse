import type { DiagramEditorNode, EdgeTypes, NodeTypes } from '../types';

const ALLOWED_OUTPUT_EDGES: Record<NodeTypes, Set<EdgeTypes>> = {
  buffer: new Set<EdgeTypes>(['bufferKey', 'bufferSeq']),
  buffer_access: new Set<EdgeTypes>(['default']),
  fork_clone: new Set<EdgeTypes>(['default']),
  fork_result: new Set<EdgeTypes>(['forkResultOk', 'forkResultErr']),
  join: new Set<EdgeTypes>(['default']),
  listen: new Set<EdgeTypes>(['default']),
  node: new Set<EdgeTypes>(['default', 'streamOut']),
  scope: new Set<EdgeTypes>(['default', 'streamOut']),
  section: new Set<EdgeTypes>(['default']),
  serialized_join: new Set<EdgeTypes>(['default']),
  split: new Set<EdgeTypes>(['splitKey', 'splitSeq', 'splitRemaining']),
  start: new Set<EdgeTypes>(['default']),
  stream_out: new Set<EdgeTypes>([]),
  terminate: new Set<EdgeTypes>([]),
  transform: new Set<EdgeTypes>(['default']),
  unzip: new Set<EdgeTypes>(['unzip']),
};

const ALLOWED_INPUT_EDGES: Record<NodeTypes, Set<EdgeTypes>> = {
  buffer: new Set<EdgeTypes>(['default']),
  buffer_access: new Set<EdgeTypes>(['default']),
  fork_clone: new Set<EdgeTypes>(['default']),
  fork_result: new Set<EdgeTypes>(['default']),
  join: new Set<EdgeTypes>(['bufferKey', 'bufferSeq']),
  listen: new Set<EdgeTypes>(['bufferKey', 'bufferSeq']),
  node: new Set<EdgeTypes>(['default']),
  scope: new Set<EdgeTypes>(['default']),
  section: new Set<EdgeTypes>(['default']),
  serialized_join: new Set<EdgeTypes>(['bufferKey', 'bufferSeq']),
  split: new Set<EdgeTypes>(['default']),
  start: new Set<EdgeTypes>([]),
  stream_out: new Set<EdgeTypes>(['streamOut']),
  terminate: new Set<EdgeTypes>(['default']),
  transform: new Set<EdgeTypes>(['default']),
  unzip: new Set<EdgeTypes>(['default']),
};

function setIntersection<T>(a: Set<T>, b: Set<T>): Set<T> {
  const intersection = new Set<T>();
  for (const elem of a) {
    if (b.has(elem)) {
      intersection.add(elem);
    }
  }
  return intersection;
}

export function allowEdges(
  source: DiagramEditorNode,
  target: DiagramEditorNode,
): EdgeTypes[] {
  const allowed_output_edges = source.type
    ? ALLOWED_OUTPUT_EDGES[source.type]
    : new Set([]);
  const allowed_input_edges = target.type
    ? ALLOWED_INPUT_EDGES[target.type]
    : new Set([]);
  return Array.from(setIntersection(allowed_output_edges, allowed_input_edges));
}
