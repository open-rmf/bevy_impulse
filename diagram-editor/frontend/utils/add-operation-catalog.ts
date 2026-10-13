import type { NodeAddChange, XYPosition } from '@xyflow/react';
import { getSchemaConfigDefaults } from '../forms/schema-config-form';
import type { NodeManager } from '../node-manager';
import {
  createOperationNode,
  createScopeNode,
  createSectionBufferNode,
  createSectionInputNode,
  createSectionOutputNode,
  type DiagramEditorNode,
  isSectionBufferNode,
  isSectionInputNode,
  isSectionOutputNode,
} from '../nodes';
import type {
  DiagramElementMetadata,
  DiagramOperation,
  NextOperation,
} from '../types/api';
import { getValidEdgeTypes } from './connection';
import { ROOT_NAMESPACE } from './namespace';
import { addUniqueSuffix } from './unique-value';

export type AddOperationKey =
  | 'sectionInput'
  | 'sectionOutput'
  | 'sectionBuffer'
  | 'node'
  | 'fork_clone'
  | 'unzip'
  | 'fork_result'
  | 'split'
  | 'join'
  | 'transform'
  | 'buffer'
  | 'buffer_access'
  | 'listen'
  | 'stream_out'
  | 'scope'
  | 'section'
  | 'script';

export type RegistryNodeKey = `node:${string}`;
export type AddOperationCandidateKey = AddOperationKey | RegistryNodeKey;

export type AddOperationCandidate = {
  key: AddOperationCandidateKey;
  label: string;
  templateOnlyRoot?: boolean;
  createPreviewNode: (
    namespace: string,
    parentId: string | undefined,
  ) => DiagramEditorNode;
  createChanges: (args: {
    namespace: string;
    parentId: string | undefined;
    newNodePosition: XYPosition;
    nodeManager: NodeManager;
  }) => NodeAddChange<DiagramEditorNode>[];
};

export type AddOperationDefinition = AddOperationCandidate & {
  key: AddOperationKey;
};

function createSectionInputChange(
  remappedId: string,
  targetId: NextOperation,
  position: XYPosition,
): NodeAddChange<DiagramEditorNode> {
  return {
    type: 'add',
    item: createSectionInputNode(remappedId, targetId, position),
  };
}

function createSectionOutputChange(
  outputId: string,
  position: XYPosition,
): NodeAddChange<DiagramEditorNode> {
  return {
    type: 'add',
    item: createSectionOutputNode(outputId, position),
  };
}

function createSectionBufferChange(
  remappedId: string,
  targetId: NextOperation,
  position: XYPosition,
): NodeAddChange<DiagramEditorNode> {
  return {
    type: 'add',
    item: createSectionBufferNode(remappedId, targetId, position),
  };
}

function createNodeChange(
  namespace: string,
  parentId: string | undefined,
  newNodePosition: XYPosition,
  op: DiagramOperation,
): NodeAddChange<DiagramEditorNode>[] {
  if (op.type === 'scope') {
    return createScopeNode(namespace, parentId, newNodePosition, op).map(
      (node) => ({ type: 'add', item: node }),
    );
  }

  return [
    {
      type: 'add',
      item: createOperationNode(namespace, parentId, newNodePosition, op),
    },
  ];
}

export const ADD_OPERATION_DEFINITIONS: AddOperationDefinition[] = [
  {
    key: 'sectionInput',
    label: 'Section Input',
    templateOnlyRoot: true,
    createPreviewNode: () =>
      createSectionInputNode(
        'preview_input',
        { builtin: 'dispose' },
        { x: 0, y: 0 },
      ),
    createChanges: ({ newNodePosition, nodeManager }) => {
      const remappedId = addUniqueSuffix(
        'new_input',
        nodeManager.nodes
          .filter(isSectionInputNode)
          .map((n) => n.data.remappedId),
      );
      return [
        createSectionInputChange(
          remappedId,
          { builtin: 'dispose' },
          newNodePosition,
        ),
      ];
    },
  },
  {
    key: 'sectionOutput',
    label: 'Section Output',
    templateOnlyRoot: true,
    createPreviewNode: () =>
      createSectionOutputNode('preview_output', { x: 0, y: 0 }),
    createChanges: ({ newNodePosition, nodeManager }) => {
      const outputId = addUniqueSuffix(
        'new_output',
        nodeManager.nodes
          .filter(isSectionOutputNode)
          .map((n) => n.data.outputId),
      );
      return [createSectionOutputChange(outputId, newNodePosition)];
    },
  },
  {
    key: 'sectionBuffer',
    label: 'Section Buffer',
    templateOnlyRoot: true,
    createPreviewNode: () =>
      createSectionBufferNode(
        'preview_buffer',
        { builtin: 'dispose' },
        { x: 0, y: 0 },
      ),
    createChanges: ({ newNodePosition, nodeManager }) => {
      const remappedId = addUniqueSuffix(
        'new_buffer',
        nodeManager.nodes
          .filter(isSectionBufferNode)
          .map((n) => n.data.remappedId),
      );
      return [
        createSectionBufferChange(
          remappedId,
          { builtin: 'dispose' },
          newNodePosition,
        ),
      ];
    },
  },
  {
    key: 'node',
    label: 'Node',
    createPreviewNode: (namespace, parentId) =>
      createOperationNode(
        namespace,
        parentId,
        { x: 0, y: 0 },
        { type: 'node', builder: '', next: { builtin: 'dispose' } },
        'preview_node',
      ),
    createChanges: ({ namespace, parentId, newNodePosition }) =>
      createNodeChange(namespace, parentId, newNodePosition, {
        type: 'node',
        builder: '',
        next: { builtin: 'dispose' },
      }),
  },
  {
    key: 'fork_clone',
    label: 'Fork Clone',
    createPreviewNode: (namespace, parentId) =>
      createOperationNode(
        namespace,
        parentId,
        { x: 0, y: 0 },
        { type: 'fork_clone', next: [] },
        'preview_fork_clone',
      ),
    createChanges: ({ namespace, parentId, newNodePosition }) =>
      createNodeChange(namespace, parentId, newNodePosition, {
        type: 'fork_clone',
        next: [],
      }),
  },
  {
    key: 'unzip',
    label: 'Unzip',
    createPreviewNode: (namespace, parentId) =>
      createOperationNode(
        namespace,
        parentId,
        { x: 0, y: 0 },
        { type: 'unzip', next: [] },
        'preview_unzip',
      ),
    createChanges: ({ namespace, parentId, newNodePosition }) =>
      createNodeChange(namespace, parentId, newNodePosition, {
        type: 'unzip',
        next: [],
      }),
  },
  {
    key: 'fork_result',
    label: 'Fork Result',
    createPreviewNode: (namespace, parentId) =>
      createOperationNode(
        namespace,
        parentId,
        { x: 0, y: 0 },
        {
          type: 'fork_result',
          err: { builtin: 'dispose' },
          ok: { builtin: 'dispose' },
        },
        'preview_fork_result',
      ),
    createChanges: ({ namespace, parentId, newNodePosition }) =>
      createNodeChange(namespace, parentId, newNodePosition, {
        type: 'fork_result',
        err: { builtin: 'dispose' },
        ok: { builtin: 'dispose' },
      }),
  },
  {
    key: 'split',
    label: 'Split',
    createPreviewNode: (namespace, parentId) =>
      createOperationNode(
        namespace,
        parentId,
        { x: 0, y: 0 },
        { type: 'split' },
        'preview_split',
      ),
    createChanges: ({ namespace, parentId, newNodePosition }) =>
      createNodeChange(namespace, parentId, newNodePosition, {
        type: 'split',
      }),
  },
  {
    key: 'join',
    label: 'Join',
    createPreviewNode: (namespace, parentId) =>
      createOperationNode(
        namespace,
        parentId,
        { x: 0, y: 0 },
        { type: 'join', buffers: [], next: { builtin: 'dispose' } },
        'preview_join',
      ),
    createChanges: ({ namespace, parentId, newNodePosition }) =>
      createNodeChange(namespace, parentId, newNodePosition, {
        type: 'join',
        buffers: [],
        next: { builtin: 'dispose' },
      }),
  },
  {
    key: 'transform',
    label: 'Transform',
    createPreviewNode: (namespace, parentId) =>
      createOperationNode(
        namespace,
        parentId,
        { x: 0, y: 0 },
        { type: 'transform', cel: '', next: { builtin: 'dispose' } },
        'preview_transform',
      ),
    createChanges: ({ namespace, parentId, newNodePosition }) =>
      createNodeChange(namespace, parentId, newNodePosition, {
        type: 'transform',
        cel: '',
        next: { builtin: 'dispose' },
      }),
  },
  {
    key: 'buffer',
    label: 'Buffer',
    createPreviewNode: (namespace, parentId) =>
      createOperationNode(
        namespace,
        parentId,
        { x: 0, y: 0 },
        { type: 'buffer' },
        'preview_buffer_node',
      ),
    createChanges: ({ namespace, parentId, newNodePosition }) =>
      createNodeChange(namespace, parentId, newNodePosition, {
        type: 'buffer',
      }),
  },
  {
    key: 'buffer_access',
    label: 'Buffer Access',
    createPreviewNode: (namespace, parentId) =>
      createOperationNode(
        namespace,
        parentId,
        { x: 0, y: 0 },
        { type: 'buffer_access', buffers: [], next: { builtin: 'dispose' } },
        'preview_buffer_access',
      ),
    createChanges: ({ namespace, parentId, newNodePosition }) =>
      createNodeChange(namespace, parentId, newNodePosition, {
        type: 'buffer_access',
        buffers: [],
        next: { builtin: 'dispose' },
      }),
  },
  {
    key: 'listen',
    label: 'Listen',
    createPreviewNode: (namespace, parentId) =>
      createOperationNode(
        namespace,
        parentId,
        { x: 0, y: 0 },
        { type: 'listen', buffers: [], next: { builtin: 'dispose' } },
        'preview_listen',
      ),
    createChanges: ({ namespace, parentId, newNodePosition }) =>
      createNodeChange(namespace, parentId, newNodePosition, {
        type: 'listen',
        buffers: [],
        next: { builtin: 'dispose' },
      }),
  },
  {
    key: 'stream_out',
    label: 'Stream Out',
    createPreviewNode: (namespace, parentId) =>
      createOperationNode(
        namespace,
        parentId,
        { x: 0, y: 0 },
        { type: 'stream_out', name: '' },
        'preview_stream_out',
      ),
    createChanges: ({ namespace, parentId, newNodePosition }) =>
      createNodeChange(namespace, parentId, newNodePosition, {
        type: 'stream_out',
        name: '',
      }),
  },
  {
    key: 'scope',
    label: 'Scope',
    createPreviewNode: (namespace, parentId) =>
      createScopeNode(
        namespace,
        parentId,
        { x: 0, y: 0 },
        {
          type: 'scope',
          start: { builtin: 'dispose' },
          ops: {},
          next: { builtin: 'dispose' },
        },
        'preview_scope',
      )[0],
    createChanges: ({ namespace, parentId, newNodePosition }) =>
      createNodeChange(namespace, parentId, newNodePosition, {
        type: 'scope',
        start: { builtin: 'dispose' },
        ops: {},
        next: { builtin: 'dispose' },
      }),
  },
  {
    key: 'section',
    label: 'Section',
    createPreviewNode: (namespace, parentId) =>
      createOperationNode(
        namespace,
        parentId,
        { x: 0, y: 0 },
        { type: 'section', template: '' },
        'preview_section',
      ),
    createChanges: ({ namespace, parentId, newNodePosition }) =>
      createNodeChange(namespace, parentId, newNodePosition, {
        type: 'section',
        template: '',
      }),
  },
  {
    key: 'script',
    label: 'Script',
    createPreviewNode: (namespace, parentId) =>
      createOperationNode(
        namespace,
        parentId,
        { x: 0, y: 0 },
        {
          type: 'script',
          environment: '',
          run: '',
          next: { builtin: 'dispose' },
        },
        'preview_script',
      ),
    createChanges: ({ namespace, parentId, newNodePosition }) =>
      createNodeChange(namespace, parentId, newNodePosition, {
        type: 'script',
        environment: '',
        run: '',
        next: { builtin: 'dispose' },
      }),
  },
];

export function getVisibleAddOperations(options: {
  isTemplateMode: boolean;
  namespace: string;
}) {
  return ADD_OPERATION_DEFINITIONS.filter((definition) => {
    if (!definition.templateOnlyRoot) {
      return true;
    }

    return options.isTemplateMode && options.namespace === ROOT_NAMESPACE;
  });
}

export function filterCompatibleAddOperations<T extends AddOperationCandidate>(
  definitions: T[],
  anchorNode: DiagramEditorNode,
  anchorHandle: string | null | undefined,
  options: { namespace: string; parentId: string | undefined },
  anchorHandleType: 'source' | 'target' = 'source',
) {
  return definitions.filter((definition) => {
    const previewNode = definition.createPreviewNode(
      options.namespace,
      options.parentId,
    );
    if (anchorHandleType === 'target' && previewNode.type === 'fork_result') {
      return false;
    }
    return anchorHandleType === 'source'
      ? getValidEdgeTypes(anchorNode, anchorHandle, previewNode, null).length >
          0
      : getValidEdgeTypes(previewNode, null, anchorNode, anchorHandle).length >
          0;
  });
}

export function getRegistryNodeBuilderCandidates(
  registry: DiagramElementMetadata,
): AddOperationCandidate[] {
  return Object.entries(registry.nodes)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([builder, metadata]) => ({
      key: `node:${builder}`,
      label: metadata.default_display_text || builder,
      createPreviewNode: (namespace, parentId) =>
        createOperationNode(
          namespace,
          parentId,
          { x: 0, y: 0 },
          { type: 'node', builder, next: { builtin: 'dispose' } },
          `preview_node_${builder}`,
        ),
      createChanges: ({ namespace, parentId, newNodePosition }) => {
        const config = getSchemaConfigDefaults(
          metadata.config_schema,
          registry.schemas,
        );
        return createNodeChange(namespace, parentId, newNodePosition, {
          type: 'node',
          builder,
          ...(config !== undefined && { config }),
          next: { builtin: 'dispose' },
        });
      },
    }));
}

export function getAddOperationCandidates(
  registry: DiagramElementMetadata,
  options: {
    includeGenericNode?: boolean;
    includeRegistryNodes?: boolean;
    includeBuiltins?: boolean;
    builtins?: AddOperationCandidate[];
  } = {},
): AddOperationCandidate[] {
  const {
    includeGenericNode = true,
    includeRegistryNodes = false,
    includeBuiltins = true,
    builtins = ADD_OPERATION_DEFINITIONS,
  } = options;

  const candidates: AddOperationCandidate[] = [];
  if (includeBuiltins) {
    candidates.push(
      ...builtins.filter(
        (definition) => includeGenericNode || definition.key !== 'node',
      ),
    );
  }

  if (includeRegistryNodes) {
    candidates.push(...getRegistryNodeBuilderCandidates(registry));
  }

  return candidates;
}
