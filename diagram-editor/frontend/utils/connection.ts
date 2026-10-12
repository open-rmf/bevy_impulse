import type { Connection } from '@xyflow/react';
import {
  createBaseEdge,
  type DiagramEditorEdge,
  EDGE_CATEGORIES,
  EdgeCategory,
  type EdgeTypes,
} from '../edges';
import { defaultEdgeData } from '../forms/edit-edge-form';
import { HandleId } from '../handles';
import type { NodeManager } from '../node-manager';
import {
  type DiagramEditorNode,
  isOperationNode,
  type NodeTypes,
} from '../nodes';
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
  script: ['default', 'streamOut'],
  section: ['section', 'buffer'],
  sectionInput: ['default'],
  sectionOutput: [],
  sectionBuffer: ['default'],
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
  script: [EdgeCategory.Data],
  section: [EdgeCategory.Data],
  sectionInput: [],
  sectionOutput: [EdgeCategory.Data],
  sectionBuffer: [],
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
  if (sourceNode.type === 'sectionBuffer') {
    return !sourceHandle &&
      !targetHandle &&
      (targetNode.type === 'buffer' || targetNode.type === 'section')
      ? ['default']
      : [];
  }
  let allowedOutputEdges: EdgeTypes[] =
    ALLOWED_OUTPUT_EDGES[sourceNode.type as NodeTypes];
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
    ALLOWED_INPUT_EDGE_CATEGORIES[targetNode.type as NodeTypes];
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
    return CardinalityType.Many;
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
    case 'listen':
    case 'scope':
    case 'script':
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

export type ValidationError = { valid: false; error: string };

export type ConnectionValidationResult = { valid: true } | ValidationError;

export type EdgeValidationResult =
  | { valid: true; validEdgeTypes: EdgeTypes[] }
  | ValidationError;

export type EdgeCreationResult =
  | { valid: true; edge: DiagramEditorEdge }
  | ValidationError;

function createValidationError(error: string): ValidationError {
  return { valid: false, error };
}

export function createConnectionFromDraggedHandle(args: {
  fromNodeId: string;
  fromHandleId: string | null | undefined;
  fromHandleType: 'source' | 'target';
  otherNodeId: string;
  otherHandleId: string | null | undefined;
}): Connection {
  return args.fromHandleType === 'source'
    ? {
        source: args.fromNodeId,
        sourceHandle: args.fromHandleId || null,
        target: args.otherNodeId,
        targetHandle: args.otherHandleId || null,
      }
    : {
        source: args.otherNodeId,
        sourceHandle: args.otherHandleId || null,
        target: args.fromNodeId,
        targetHandle: args.fromHandleId || null,
      };
}

export interface DraggedHandleRef {
  nodeId: string;
  id?: string | null;
  type: 'source' | 'target';
}

export function createConnectionFromHandles(
  fromHandle: DraggedHandleRef,
  otherNodeId: string,
  otherHandleId: string | null | undefined,
): Connection {
  return createConnectionFromDraggedHandle({
    fromNodeId: fromHandle.nodeId,
    fromHandleId: fromHandle.id,
    fromHandleType: fromHandle.type,
    otherNodeId,
    otherHandleId,
  });
}

export function validateDraggedHandlePair(args: {
  fromHandleType: 'source' | 'target';
  otherHandleType: 'source' | 'target';
}): ConnectionValidationResult {
  if (args.fromHandleType === args.otherHandleType) {
    return createValidationError(
      args.fromHandleType === 'source'
        ? 'Cannot connect an output to another output'
        : 'Cannot connect an input to another input',
    );
  }

  return { valid: true };
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
 * Perform a quick check if connection is valid.
 * This only checks if there is a valid edge type, does not check for conflicting edges, data correctness etc.
 *
 * Complexity is O(1).
 *
 * validateEdgeQuick checks if a given edge is valid while validateConnectionQuick check if there
 * is at least 1 valid edge type for a given connection.
 */
export function validateConnectionQuick(
  conn: Connection | DiagramEditorEdge,
  nodeManager: NodeManager,
): ConnectionValidationResult {
  const sourceNode = nodeManager.tryGetNode(conn.source);
  const targetNode = nodeManager.tryGetNode(conn.target);

  if (!sourceNode || !targetNode) {
    return createValidationError('cannot find source or target node');
  }

  if (
    (sourceNode.parentId || targetNode.parentId) &&
    sourceNode.parentId !== targetNode.parentId
  ) {
    return createValidationError(
      'source and target nodes are in different sections',
    );
  }

  const validEdgeTypes = getValidEdgeTypes(
    sourceNode,
    conn.sourceHandle,
    targetNode,
    conn.targetHandle,
  );
  if (validEdgeTypes.length === 0) {
    return createValidationError('no valid edge type');
  }

  return { valid: true };
}

export function validateSourceOutputCapacity(
  sourceNode: DiagramEditorNode,
  sourceHandle: string | null | undefined,
  edges: DiagramEditorEdge[],
  edgeId?: string,
): ConnectionValidationResult {
  // Check if the source supports emitting multiple outputs.
  // NOTE: All nodes supports "Many" inputs so we don't need to check that.
  const outputCardinality = getOutputCardinality(sourceNode.type, sourceHandle);
  switch (outputCardinality) {
    case CardinalityType.Single: {
      if (
        edges.some(
          (e) =>
            e.source === sourceNode.id &&
            (sourceHandle || null) === (e.sourceHandle || null) &&
            edgeId !== e.id,
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
        if (e.source === sourceNode.id && edgeId !== e.id) {
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

  return { valid: true };
}

export function validateConnectionSimple(
  conn: Connection | DiagramEditorEdge,
  nodeManager: NodeManager,
  edges: DiagramEditorEdge[],
): ConnectionValidationResult {
  const quickCheck = validateConnectionQuick(conn, nodeManager);
  if (!quickCheck.valid) {
    return quickCheck;
  }

  const sourceNode = nodeManager.tryGetNode(conn.source);
  if (!sourceNode) {
    return createValidationError('cannot find source or target node');
  }

  if ('type' in conn) {
    const slot = outputSlot(conn);
    if (
      slot !== undefined &&
      edges.some(
        (edge) =>
          edge.id !== conn.id &&
          edge.source === conn.source &&
          outputSlot(edge) === slot,
      )
    ) {
      return createValidationError(
        'This output slot is already connected to another input',
      );
    }
  }

  return validateSourceOutputCapacity(
    sourceNode,
    conn.sourceHandle,
    edges,
    'id' in conn ? conn.id : undefined,
  );
}

function outputSlot(edge: DiagramEditorEdge): string | undefined {
  switch (edge.type) {
    case 'unzip':
    case 'splitSeq':
      return `${edge.type}:${edge.data.output.seq}`;
    case 'splitKey':
      return `splitKey:${edge.data.output.key}`;
    case 'splitRemaining':
    case 'forkResultOk':
    case 'forkResultErr':
      return edge.type;
    case 'streamOut':
      return edge.data.output.streamId
        ? `streamOut:${edge.data.output.streamId}`
        : undefined;
    case 'section':
      return edge.data.output.output
        ? `section:${edge.data.output.output}`
        : undefined;
    default:
      return undefined;
  }
}

function nextBufferKey(
  sourceNode: DiagramEditorNode,
  existingKeys: Set<string>,
): string {
  const baseKey = isOperationNode(sourceNode) ? sourceNode.data.opId : 'buffer';
  if (!existingKeys.has(baseKey)) {
    return baseKey;
  }

  let suffix = 1;
  while (existingKeys.has(`${baseKey}_${suffix}`)) {
    suffix++;
  }
  return `${baseKey}_${suffix}`;
}

function adjustBufferEdgeInputForTarget(
  edge: DiagramEditorEdge,
  sourceNode: DiagramEditorNode,
  targetNode: DiagramEditorNode,
  edges: DiagramEditorEdge[],
) {
  if (
    edge.type !== 'buffer' ||
    !isOperationNode(targetNode) ||
    (targetNode.type !== 'buffer_access' &&
      targetNode.type !== 'join' &&
      targetNode.type !== 'listen')
  ) {
    return;
  }

  const inputs = edges
    .filter(
      (existing) =>
        existing.type === 'buffer' &&
        existing.target === targetNode.id &&
        existing.id !== edge.id,
    )
    .map((existing) => existing.data.input);
  const keyed = inputs.length
    ? inputs.some((input) => input.type === 'bufferKey')
    : !Array.isArray(targetNode.data.op.buffers);
  if (keyed) {
    edge.data.input = {
      type: 'bufferKey',
      key: nextBufferKey(
        sourceNode,
        new Set(
          inputs.flatMap((input) =>
            input.type === 'bufferKey' ? [input.key] : [],
          ),
        ),
      ),
    };
  } else {
    const used = new Set(
      inputs.flatMap((input) =>
        input.type === 'bufferSeq' ? [input.seq] : [],
      ),
    );
    let seq = 0;
    while (used.has(seq)) seq++;
    edge.data.input = {
      type: 'bufferSeq',
      seq,
    };
  }
}

export function createEdgeFromConnection(
  conn: Connection,
  nodeManager: NodeManager,
  edges: DiagramEditorEdge[],
  id?: string,
): EdgeCreationResult {
  const sourceNode = nodeManager.tryGetNode(conn.source);
  const targetNode = nodeManager.tryGetNode(conn.target);
  if (!sourceNode || !targetNode) {
    return createValidationError('cannot find source or target node');
  }

  const validEdges = getValidEdgeTypes(
    sourceNode,
    conn.sourceHandle,
    targetNode,
    conn.targetHandle,
  );
  if (validEdges.length === 0) {
    return createValidationError(
      `cannot connect "${sourceNode.type}" to "${targetNode.type}"`,
    );
  }

  const previous = edges.find((edge) => edge.id === id);
  const preserveOutput =
    previous?.source === conn.source &&
    (previous.sourceHandle || null) === (conn.sourceHandle || null);
  const sourceEdges = edges.filter(
    (edge) => edge.source === conn.source && edge.id !== id,
  );
  let edgeType =
    previous && validEdges.includes(previous.type)
      ? previous.type
      : validEdges[0];
  if (preserveOutput) {
    if (!validEdges.includes(previous.type)) {
      return createValidationError(
        'The selected output cannot connect to this input',
      );
    }
  } else if (
    !(previous && validEdges.includes(previous.type)) &&
    sourceNode.type === 'split' &&
    !sourceEdges.some((edge) => edge.type === 'splitKey') &&
    (sourceEdges.some((edge) => edge.type === 'splitSeq') ||
      (sourceNode.data.op.sequential &&
        !Object.keys(sourceNode.data.op.keyed ?? {}).length))
  ) {
    edgeType = 'splitSeq';
  }

  const newEdge = {
    ...createBaseEdge(
      conn.source,
      conn.sourceHandle,
      conn.target,
      conn.targetHandle,
      id,
    ),
    type: edgeType,
    data: defaultEdgeData(edgeType),
  } as DiagramEditorEdge;

  if (preserveOutput) {
    newEdge.data.output = { ...previous.data.output };
  } else if (newEdge.type === 'unzip' || newEdge.type === 'splitSeq') {
    const used = new Set(
      sourceEdges.flatMap((edge) =>
        edge.type === newEdge.type &&
        (edge.type === 'unzip' || edge.type === 'splitSeq')
          ? [edge.data.output.seq]
          : [],
      ),
    );
    let seq = 0;
    while (used.has(seq)) seq++;
    newEdge.data.output = { seq };
  } else if (newEdge.type === 'splitKey') {
    const used = new Set(
      sourceEdges.flatMap((edge) =>
        edge.type === 'splitKey' ? [edge.data.output.key] : [],
      ),
    );
    let key = 'unnamed_key';
    let suffix = 1;
    while (used.has(key)) key = `unnamed_key_${suffix++}`;
    newEdge.data.output = { key };
  }

  if (targetNode.type === 'section') {
    if (sourceNode.type === 'sectionBuffer') {
      newEdge.data.input = {
        type: 'sectionBuffer',
        inputId: '',
      };
    } else if (EDGE_CATEGORIES[newEdge.type] === EdgeCategory.Data) {
      newEdge.data.input = {
        type: 'sectionInput',
        inputId: '',
      };
    }
  }

  adjustBufferEdgeInputForTarget(newEdge, sourceNode, targetNode, edges);

  if (
    previous?.target === conn.target &&
    (previous.targetHandle || null) === (conn.targetHandle || null) &&
    EDGE_CATEGORIES[previous.type] === EDGE_CATEGORIES[newEdge.type] &&
    (sourceNode.type !== 'sectionBuffer' ||
      targetNode.type !== 'section' ||
      previous.data.input.type === 'sectionBuffer')
  ) {
    newEdge.data.input = { ...previous.data.input };
  }

  return { valid: true, edge: newEdge };
}

/**
 * Perform a simple check of the validity of edges.
 * Includes the checks in `validateEdgeQuick` and the following:
 *   * Check that the number of output edges does not exceed what the node allows.
 *   * Reject conflicting output slots and buffer consumer selections.
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
  if (
    (edge.type === 'unzip' || edge.type === 'splitSeq') &&
    (!Number.isInteger(edge.data.output.seq) || edge.data.output.seq < 0)
  ) {
    return createValidationError(
      'A sequential output index must be a non-negative integer',
    );
  }

  const sourceNode = nodeManager.tryGetNode(edge.source);
  const targetNode = nodeManager.tryGetNode(edge.target);
  if (!sourceNode || !targetNode) {
    return createValidationError('cannot find source or target node');
  }

  if (targetNode.type === 'section') {
    if (
      edge.data.input.type !== 'sectionInput' &&
      edge.data.input.type !== 'sectionBuffer'
    ) {
      return createValidationError(
        'target is a section but there is no input slot',
      );
    }
    if (
      sourceNode.type === 'sectionBuffer' &&
      edge.data.input.type !== 'sectionBuffer'
    ) {
      return createValidationError(
        'An exposed buffer must alias a buffer inside the section',
      );
    }
  }

  if (edge.type === 'buffer') {
    const input = edge.data.input;
    if (input.type !== 'bufferKey' && input.type !== 'bufferSeq') {
      return createValidationError(
        'A buffer consumer needs a keyed or sequential buffer slot',
      );
    }
    if (
      input.type === 'bufferSeq' &&
      (!Number.isInteger(input.seq) || input.seq < 0)
    ) {
      return createValidationError(
        'A sequential buffer index must be a non-negative integer',
      );
    }
    const sequentialSlots = new Map<number, number>();
    if (input.type === 'bufferSeq') sequentialSlots.set(input.seq, 1);
    for (const other of edges) {
      if (
        other.id === edge.id ||
        other.type !== 'buffer' ||
        other.target !== edge.target
      )
        continue;
      if (other.data.input.type !== input.type) {
        return createValidationError(
          'Buffer connections to one consumer must use the same slot type',
        );
      }
      if (
        (input.type === 'bufferKey' &&
          other.data.input.type === 'bufferKey' &&
          input.key === other.data.input.key) ||
        (input.type === 'bufferSeq' &&
          other.data.input.type === 'bufferSeq' &&
          input.seq === other.data.input.seq)
      ) {
        return createValidationError(
          'This buffer input slot is already connected',
        );
      }
      if (
        other.data.input.type === 'bufferSeq' &&
        validateEdgeQuick(other, nodeManager).valid &&
        validateConnectionQuick(other, nodeManager).valid
      ) {
        const seq = other.data.input.seq;
        sequentialSlots.set(seq, (sequentialSlots.get(seq) ?? 0) + 1);
      }
    }
    if (input.type === 'bufferSeq') {
      let firstMissing = 0;
      // Duplicate or invalid earlier slots will be excluded from diagnostic
      // export, so they cannot fill a gap in the serialized buffer selection.
      while (sequentialSlots.get(firstMissing) === 1) firstMissing++;
      if (input.seq > firstMissing) {
        return createValidationError(
          `Sequential buffer slot ${firstMissing} must be connected first`,
        );
      }
    }
  }

  const outputCapacity = validateConnectionSimple(edge, nodeManager, edges);
  if (!outputCapacity.valid) {
    return outputCapacity;
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
