import type { ReactFlowInstance } from '@xyflow/react';
import type { DiagramEditorEdge, EdgeTypes } from '../edges';
import type { DiagramEditorNode, NodeTypes } from '../nodes';
import { exhaustiveCheck } from './exhaustive-check';

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
  sectionInput: new Set<EdgeTypes>(['default']),
  sectionOutput: new Set<EdgeTypes>([]),
  sectionBuffer: new Set<EdgeTypes>(['default']),
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
  sectionInput: new Set<EdgeTypes>([]),
  sectionOutput: new Set<EdgeTypes>(['default']),
  sectionBuffer: new Set<EdgeTypes>([]),
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

export function getValidEdgeTypes(
  sourceNode: DiagramEditorNode,
  targetNode: DiagramEditorNode,
): EdgeTypes[] {
  const allowedOutputEdges = sourceNode.type
    ? ALLOWED_OUTPUT_EDGES[sourceNode.type]
    : new Set([]);
  const allowedInputEdges = targetNode.type
    ? ALLOWED_INPUT_EDGES[targetNode.type]
    : new Set([]);
  return Array.from(setIntersection(allowedOutputEdges, allowedInputEdges));
}

enum CardinalityType {
  Single,
  Pair,
  Many,
}

function getOutputCardinality(type: NodeTypes): CardinalityType {
  switch (type) {
    case 'fork_clone':
    case 'unzip':
    case 'buffer':
    case 'section':
    case 'split': {
      return CardinalityType.Many;
    }
    case 'fork_result': {
      return CardinalityType.Pair;
    }
    case 'node':
    case 'buffer_access':
    case 'join':
    case 'serialized_join':
    case 'listen':
    case 'scope':
    case 'stream_out':
    case 'transform':
    case 'start':
    case 'terminate':
    case 'sectionBuffer':
    case 'sectionInput':
    case 'sectionOutput': {
      return CardinalityType.Single;
    }
    default: {
      exhaustiveCheck(type);
      throw new Error('unknown op type');
    }
  }
}

export type EdgeValidationResult =
  | { valid: true; validEdgeTypes: EdgeTypes[] }
  | { valid: false; error: string };

function createValidationError(error: string): EdgeValidationResult {
  return { valid: false, error };
}

/**
 * Perform a quick check if an edge is valid.
 * This only checks if the edge type is valid, does not check for conflicting edges, data correctness etc.
 * Complexity is O(1).
 */
export function checkValidEdgeQuick(
  edge: DiagramEditorEdge,
  reactFlow: ReactFlowInstance<DiagramEditorNode, DiagramEditorEdge>,
): EdgeValidationResult {
  const sourceNode = reactFlow.getNode(edge.source);
  const targetNode = reactFlow.getNode(edge.target);

  if (!sourceNode || !targetNode) {
    return createValidationError('cannot find source or target node');
  }

  const validEdgeTypes = getValidEdgeTypes(sourceNode, targetNode);
  if (!validEdgeTypes.includes(edge.type)) {
    return createValidationError('invalid edge type');
  }

  return { valid: true, validEdgeTypes };
}

/**
 * Perform a simple check of the validity of edges.
 * A more complete check than `checkValidEdgeQuick`, but still does not do complicated checks like type compatibility.
 * Complexity is O(numOfEdges), so it is not recommended to call this very frequently.
 */
export function checkValidEdgeSimple(
  edge: DiagramEditorEdge,
  reactFlow: ReactFlowInstance<DiagramEditorNode, DiagramEditorEdge>,
): EdgeValidationResult {
  const quickCheck = checkValidEdgeQuick(edge, reactFlow);
  if (!quickCheck.valid) {
    return quickCheck;
  }

  const sourceNode = reactFlow.getNode(edge.source);
  if (!sourceNode) {
    return createValidationError('cannot find source or target node');
  }

  // Check if the source supports emitting multiple outputs.
  // NOTE: All nodes supports "Many" inputs so we don't need to check that.
  const outputCardinality = getOutputCardinality(sourceNode.type);
  switch (outputCardinality) {
    case CardinalityType.Single: {
      if (reactFlow.getEdges().some((edge) => edge.source === sourceNode.id)) {
        return createValidationError('source node already has an edge');
      }
      break;
    }
    case CardinalityType.Pair: {
      let count = 0;
      for (const edge of reactFlow.getEdges()) {
        if (edge.source === sourceNode.id) {
          count++;
        }
        if (count > 1) {
          return createValidationError('source node already has two edges');
        }
      }
      break;
    }
    case CardinalityType.Many: {
      break;
    }
    default: {
      exhaustiveCheck(outputCardinality);
      throw new Error('unknown output cardinality');
    }
  }

  return { valid: true, validEdgeTypes: quickCheck.validEdgeTypes };
}

/**
 * Perform a full check of the validity of edges.
 * This can be slow so it is not recommended to call this frequently.
 */
export async function checkValidEdgeFull(
  edge: DiagramEditorEdge,
  reactFlow: ReactFlowInstance<DiagramEditorNode, DiagramEditorEdge>,
): Promise<EdgeValidationResult> {
  const simpleCheck = checkValidEdgeSimple(edge, reactFlow);
  if (!simpleCheck.valid) {
    return simpleCheck;
  }

  // TODO: check message type compatibility. Writing the same logic as `bevy_impulse` is hard, it might
  // be better to introduce a validation endpoint.

  return { valid: true, validEdgeTypes: simpleCheck.validEdgeTypes };
}
