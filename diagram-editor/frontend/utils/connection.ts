import type { ReactFlowInstance } from '@xyflow/react';
import type { DiagramEditorEdge, EdgeTypes } from '../edges';
import type { DiagramEditorNode, NodeTypes } from '../nodes';
import { exhaustiveCheck } from './exhaustive-check';

enum EdgeCategory {
  Data,
  Buffer,
  Stream,
}

const EDGE_CATEGORIES: Record<EdgeTypes, EdgeCategory> = {
  buffer: EdgeCategory.Buffer,
  forkResultOk: EdgeCategory.Data,
  forkResultErr: EdgeCategory.Data,
  splitKey: EdgeCategory.Data,
  splitSeq: EdgeCategory.Data,
  splitRemaining: EdgeCategory.Data,
  default: EdgeCategory.Data,
  streamOut: EdgeCategory.Stream,
  unzip: EdgeCategory.Data,
  // section: EdgeCategory.Data,
};

const ALLOWED_OUTPUT_EDGES: Record<NodeTypes, EdgeTypes[]> = {
  buffer: ['buffer', 'buffer'],
  buffer_access: ['default'],
  fork_clone: ['default'],
  fork_result: ['forkResultOk', 'forkResultErr'],
  join: ['default'],
  listen: ['default'],
  node: ['default', 'streamOut'],
  scope: ['default', 'streamOut'],
  section: ['default'],
  sectionInput: ['default'],
  sectionOutput: [],
  sectionBuffer: ['buffer'],
  serialized_join: ['default'],
  split: ['splitKey', 'splitSeq', 'splitRemaining'],
  start: ['default'],
  stream_out: [],
  terminate: [],
  transform: ['default'],
  unzip: ['unzip'],
};

const ALLOWED_INPUT_EDGE_CATEGORIES: Record<NodeTypes, EdgeCategory[]> = {
  buffer: [EdgeCategory.Data],
  buffer_access: [EdgeCategory.Data, EdgeCategory.Buffer],
  fork_clone: [EdgeCategory.Data],
  fork_result: [EdgeCategory.Data],
  join: [EdgeCategory.Buffer, EdgeCategory.Data],
  listen: [EdgeCategory.Buffer],
  node: [EdgeCategory.Data],
  scope: [EdgeCategory.Data],
  section: [EdgeCategory.Data],
  sectionInput: [],
  sectionOutput: [EdgeCategory.Data],
  sectionBuffer: [],
  serialized_join: [EdgeCategory.Data, EdgeCategory.Buffer],
  split: [EdgeCategory.Data],
  start: [],
  stream_out: [EdgeCategory.Stream],
  terminate: [EdgeCategory.Data],
  transform: [EdgeCategory.Data],
  unzip: [EdgeCategory.Data],
};

export function getValidEdgeTypes(
  sourceNode: DiagramEditorNode,
  targetNode: DiagramEditorNode,
): EdgeTypes[] {
  const allowedOutputEdges = [...ALLOWED_OUTPUT_EDGES[sourceNode.type]];
  const allowedInputEdgeCategories =
    ALLOWED_INPUT_EDGE_CATEGORIES[targetNode.type];
  return allowedOutputEdges.filter((edgeType) =>
    allowedInputEdgeCategories.includes(EDGE_CATEGORIES[edgeType]),
  );
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
 * A minimal type for only the required accessor methods in `ReactFlowInstance`.
 * This is mostly so that tests can be written without rendering a `ReactFlow` instance.
 */
export type NodesAndEdgesAccessor = Pick<
  ReactFlowInstance<DiagramEditorNode, DiagramEditorEdge>,
  'getNode' | 'getNodes' | 'getEdges'
>;

/**
 * Perform a quick check if an edge is valid.
 * This only checks if the edge type is valid, does not check for conflicting edges, data correctness etc.
 *
 * Complexity is O(1).
 */
export function validateEdgeQuick(
  edge: DiagramEditorEdge,
  reactFlow: NodesAndEdgesAccessor,
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
 * Includes the checks in `validateEdgeQuick` and the following:
 *   * Check that the number of output edges does not exceed what the node allows.
 *     * Note that it does not check for conflicting edges, e.g. a `fork_result` with 2 "ok" edges is still valid.
 *
 * Complexity is O(numOfEdges).
 */
export function validateEdgeSimple(
  edge: DiagramEditorEdge,
  reactFlow: NodesAndEdgesAccessor,
): EdgeValidationResult {
  const quickCheck = validateEdgeQuick(edge, reactFlow);
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
      if (
        reactFlow
          .getEdges()
          .some((e) => e.source === sourceNode.id && edge.id !== e.id)
      ) {
        return createValidationError('source node already has an edge');
      }
      break;
    }
    case CardinalityType.Pair: {
      let count = 0;
      for (const e of reactFlow.getEdges()) {
        if (e.source === sourceNode.id && edge.id !== e.id) {
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
 * Includes the checks in `validateEdgesSimple` and the following:
 *   * TODO: Export and send the diagram to `bevy_impulse` for complete validation.
 *
 * This can be slow so it is not recommended to call this frequently.
 */
export async function validateEdgeFull(
  edge: DiagramEditorEdge,
  reactFlow: NodesAndEdgesAccessor,
): Promise<EdgeValidationResult> {
  const simpleCheck = validateEdgeSimple(edge, reactFlow);
  if (!simpleCheck.valid) {
    return simpleCheck;
  }

  // TODO: Writing the same logic as `bevy_impulse` to do complete validation is hard, it is
  // be better to introduce a validation endpoint and have `bevy_impulse` do the validation.

  return { valid: true, validEdgeTypes: simpleCheck.validEdgeTypes };
}
