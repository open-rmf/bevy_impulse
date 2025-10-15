import {
  type DiagramEditorEdge,
  EDGE_CATEGORIES,
  EdgeCategory,
  type EdgeTypes,
} from '../edges';
import { HandleId } from '../handles';
import type { NodeManager } from '../node-manager';
import type { DiagramEditorNode, NodeTypes } from '../nodes';
import { exhaustiveCheck } from './exhaustive-check';

/**
 * List of edge types that a node can output.
 * TODO: Consider defining for each handle, e.g.
 *
 * ```ts
 * {
 *   node: {
 *     default: 'default',
 *     dataStream: 'streamOut',
 *   }
 * }
 * ```
 */
const ALLOWED_OUTPUT_EDGES: Record<NodeTypes, EdgeTypes[]> = {
  buffer: ['buffer'],
  buffer_access: ['default'],
  fork_clone: ['default'],
  fork_result: ['forkResultOk', 'forkResultErr'],
  join: ['default'],
  listen: ['default'],
  node: ['default', 'streamOut'],
  scope: ['default', 'streamOut'],
  section: ['section'],
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
  join: [EdgeCategory.Buffer],
  listen: [EdgeCategory.Buffer],
  node: [EdgeCategory.Data],
  scope: [EdgeCategory.Data],
  section: [EdgeCategory.Data, EdgeCategory.Buffer],
  sectionInput: [],
  sectionOutput: [EdgeCategory.Data],
  sectionBuffer: [],
  serialized_join: [EdgeCategory.Buffer],
  split: [EdgeCategory.Data],
  start: [],
  stream_out: [EdgeCategory.Data],
  terminate: [EdgeCategory.Data],
  transform: [EdgeCategory.Data],
  unzip: [EdgeCategory.Data],
};

const ALLOWED_HANDLE_OUTPUT_EDGES: Record<string, EdgeTypes[]> = {
  dataStream: ['streamOut'],
  forkResultOk: ['forkResultOk'],
  forkResultErr: ['forkResultErr'],
} satisfies Record<HandleId, EdgeTypes[]>;

/// List of edge types that the default handle should not allow. These edges are expected to have their own handles.
const DISALLOWED_DEFAULT_HANDLE_OUTPUT_EDGES: EdgeTypes[] = ['streamOut'];

const ALLOWED_HANDLE_INPUT_EDGE_CATEGORIES: Record<string, EdgeCategory[]> = {
  dataStream: [EdgeCategory.Data],
  forkResultOk: [],
  forkResultErr: [],
} satisfies Record<HandleId, EdgeCategory[]>;

function arrayIntersection<T>(a: T[], b: T[]): T[] {
  const intersection = [];
  for (const elem of a) {
    if (b.includes(elem)) {
      intersection.push(elem);
    }
  }
  return intersection;
}

function arrayDifference<T>(a: T[], b: T[]): T[] {
  const result = [];
  for (const elem of a) {
    if (!b.includes(elem)) {
      result.push(elem);
    }
  }
  return result;
}

export function getValidEdgeTypes(
  sourceNode: DiagramEditorNode,
  sourceHandle: string | null | undefined,
  targetNode: DiagramEditorNode,
  targetHandle: string | null | undefined,
): EdgeTypes[] {
  let allowedOutputEdges: EdgeTypes[] = ALLOWED_OUTPUT_EDGES[sourceNode.type];
  if (sourceHandle) {
    const allowedHandleOutput = ALLOWED_HANDLE_OUTPUT_EDGES[sourceHandle];
    if (allowedHandleOutput) {
      // we are only dealing with very small arrays, no need to create a Set.
      allowedOutputEdges = arrayIntersection(
        allowedOutputEdges,
        allowedHandleOutput,
      );
    } else {
      console.error('failed to get allowed handle output edges');
    }
  } else {
    allowedOutputEdges = arrayDifference(
      allowedOutputEdges,
      DISALLOWED_DEFAULT_HANDLE_OUTPUT_EDGES,
    );
  }

  let allowedInputEdgeCategories =
    ALLOWED_INPUT_EDGE_CATEGORIES[targetNode.type];
  if (targetHandle) {
    const allowedHandleInput =
      ALLOWED_HANDLE_INPUT_EDGE_CATEGORIES[targetHandle];
    if (allowedHandleInput) {
      // we are only dealing with very small arrays, no need to create a Set.
      allowedInputEdgeCategories = arrayIntersection(
        allowedInputEdgeCategories,
        allowedHandleInput,
      );
    } else {
      console.error('failed to get allowed handle input edge categories');
    }
  }

  return Array.from(allowedOutputEdges).filter((edgeType) =>
    allowedInputEdgeCategories.includes(EDGE_CATEGORIES[edgeType]),
  );
}

enum CardinalityType {
  Single,
  Pair,
  Many,
}

function getOutputCardinality(
  type: NodeTypes,
  handleId: string | null | undefined,
): CardinalityType {
  if (handleId === HandleId.DataStream) {
    return CardinalityType.Single;
  }

  switch (type) {
    case 'fork_clone':
    case 'unzip':
    case 'buffer':
    case 'section':
    case 'split': {
      return CardinalityType.Many;
    }
    case 'fork_result':
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
 *
 * Complexity is O(1).
 */
export function validateEdgeQuick(
  edge: DiagramEditorEdge,
  nodeManager: NodeManager,
): EdgeValidationResult {
  const sourceNode = nodeManager.tryGetNode(edge.source);
  const targetNode = nodeManager.tryGetNode(edge.target);

  if (!sourceNode || !targetNode) {
    return createValidationError('cannot find source or target node');
  }

  const validEdgeTypes = getValidEdgeTypes(
    sourceNode,
    edge.sourceHandle,
    targetNode,
    edge.targetHandle,
  );
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
  nodeManager: NodeManager,
  edges: DiagramEditorEdge[],
): EdgeValidationResult {
  const quickCheck = validateEdgeQuick(edge, nodeManager);
  if (!quickCheck.valid) {
    return quickCheck;
  }

  const sourceNode = nodeManager.tryGetNode(edge.source);
  const targetNode = nodeManager.tryGetNode(edge.target);
  if (!sourceNode || !targetNode) {
    return createValidationError('cannot find source or target node');
  }

  if (targetNode.type === 'section') {
    if (
      EDGE_CATEGORIES[edge.type] === EdgeCategory.Buffer &&
      edge.data.input.type !== 'sectionBuffer'
    ) {
      return createValidationError(
        'target is a section but there is no input slot',
      );
    } else if (
      EDGE_CATEGORIES[edge.type] === EdgeCategory.Data &&
      edge.data.input.type !== 'sectionInput'
    ) {
      return createValidationError(
        'target is a section but there is no input slot',
      );
    }
  }

  // Check if the source supports emitting multiple outputs.
  // NOTE: All nodes supports "Many" inputs so we don't need to check that.
  const outputCardinality = getOutputCardinality(
    sourceNode.type,
    edge.sourceHandle,
  );
  switch (outputCardinality) {
    case CardinalityType.Single: {
      if (
        edges.some(
          (e) =>
            e.source === sourceNode.id &&
            (edge.sourceHandle || null) === (e.sourceHandle || null) &&
            edge.id !== e.id,
        )
      ) {
        return createValidationError(
          'This output can only be connected to one input',
        );
      }
      break;
    }
    case CardinalityType.Pair: {
      let count = 0;
      for (const e of edges) {
        if (e.source === sourceNode.id && edge.id !== e.id) {
          count++;
        }
        if (count > 1) {
          return createValidationError(
            'This output can only be connected to two inputs',
          );
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
 *   * TODO: Export and send the diagram to `crossflow` for complete validation.
 *
 * This can be slow so it is not recommended to call this frequently.
 */
export async function validateEdgeFull(
  edge: DiagramEditorEdge,
  nodeManager: NodeManager,
  edges: DiagramEditorEdge[],
): Promise<EdgeValidationResult> {
  const simpleCheck = validateEdgeSimple(edge, nodeManager, edges);
  if (!simpleCheck.valid) {
    return simpleCheck;
  }

  // TODO: Writing the same logic as `crossflow` to do complete validation is hard, it is
  // be better to introduce a validation endpoint and have `crossflow` do the validation.

  return { valid: true, validEdgeTypes: simpleCheck.validEdgeTypes };
}
