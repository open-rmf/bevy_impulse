import { NodeManager } from '../node-manager';
import { createOperationNode, isOperationNode } from '../nodes';
import {
  ADD_OPERATION_DEFINITIONS,
  filterCompatibleAddOperations,
  getAddOperationCandidates,
  getRegistryNodeBuilderCandidates,
  getVisibleAddOperations,
} from './add-operation-catalog';
import { ROOT_NAMESPACE } from './namespace';

describe('add operation catalog', () => {
  test('filters add-operation suggestions to compatible targets', () => {
    const sourceNode = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'buffer' },
      'source_buffer',
    );
    const compatible = filterCompatibleAddOperations(
      getVisibleAddOperations({
        isTemplateMode: false,
        namespace: ROOT_NAMESPACE,
      }),
      sourceNode,
      null,
      {
        namespace: ROOT_NAMESPACE,
        parentId: undefined,
      },
    );

    expect(compatible.some((operation) => operation.key === 'join')).toBe(true);
    expect(compatible.some((operation) => operation.key === 'listen')).toBe(
      true,
    );
    expect(compatible.some((operation) => operation.key === 'node')).toBe(
      false,
    );
    expect(new NodeManager([sourceNode]).getNode(sourceNode.id).type).toBe(
      'buffer',
    );
  });

  test('shows section interface operations only for template roots', () => {
    const normalMode = getVisibleAddOperations({
      isTemplateMode: false,
      namespace: ROOT_NAMESPACE,
    });
    const templateMode = getVisibleAddOperations({
      isTemplateMode: true,
      namespace: ROOT_NAMESPACE,
    });

    expect(
      normalMode.some((operation) => operation.key === 'sectionInput'),
    ).toBe(false);
    expect(
      templateMode.some((operation) => operation.key === 'sectionInput'),
    ).toBe(true);
  });

  test('filters add-operation suggestions for upstream sources when dragging from a target handle', () => {
    const targetNode = createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'join', buffers: [], next: { builtin: 'dispose' } },
      'target_join',
    );
    const compatible = filterCompatibleAddOperations(
      getVisibleAddOperations({
        isTemplateMode: false,
        namespace: ROOT_NAMESPACE,
      }),
      targetNode,
      null,
      {
        namespace: ROOT_NAMESPACE,
        parentId: undefined,
      },
      'target',
    );

    expect(compatible.some((operation) => operation.key === 'buffer')).toBe(
      true,
    );
    expect(
      compatible.some((operation) => operation.key === 'fork_result'),
    ).toBe(false);
    expect(compatible.some((operation) => operation.key === 'node')).toBe(
      false,
    );
  });

  test('creates concrete registered node builder candidates', () => {
    const registry = {
      messages: [],
      nodes: {
        custom_builder: {
          config_schema: { $ref: '#/$defs/CustomConfig' },
          default_display_text: 'Custom Builder',
          request: 0,
          response: 1,
          streams: {},
          config_examples: [],
        },
      },
      reverse_message_lookup: {
        result: [],
        split: [],
        unzip: [],
      },
      schemas: {
        CustomConfig: {
          type: 'object',
          properties: {
            label: { type: 'string', default: 'New node' },
            retries: { type: 'integer', default: 2 },
          },
        },
      },
      scripting: {},
      sections: {},
      trace_supported: false,
    };

    const [candidate] = getRegistryNodeBuilderCandidates(registry);
    expect(candidate.key).toBe('node:custom_builder');
    expect(candidate.label).toBe('Custom Builder');

    const [change] = candidate.createChanges({
      namespace: ROOT_NAMESPACE,
      parentId: undefined,
      newNodePosition: { x: 1, y: 2 },
      nodeManager: new NodeManager([]),
    });
    expect(change.item.type).toBe('node');
    if (change.item.type === 'node') {
      expect(change.item.data.op.builder).toBe('custom_builder');
      expect(change.item.data.op.config).toEqual({
        label: 'New node',
        retries: 2,
      });
    }

    const popupCandidates = getAddOperationCandidates(registry, {
      includeGenericNode: false,
      includeRegistryNodes: true,
    });
    expect(popupCandidates.some((operation) => operation.key === 'node')).toBe(
      false,
    );
    expect(
      popupCandidates.some(
        (operation) => operation.key === 'node:custom_builder',
      ),
    ).toBe(true);
  });
});

test.each([
  ['buffer', 'buffer'],
  ['fork_clone', 'clone'],
  ['fork_result', 'result'],
  ['scope', 'scope'],
])('new %s operations have readable default IDs', (key, prefix) => {
  const candidate = ADD_OPERATION_DEFINITIONS.find(
    (entry) => entry.key === key,
  )!;
  const [change] = candidate.createChanges({
    namespace: ROOT_NAMESPACE,
    parentId: undefined,
    newNodePosition: { x: 0, y: 0 },
    nodeManager: new NodeManager([]),
  });
  expect(change.item.data.opId).toMatch(new RegExp(`^${prefix}_[A-Z0-9]{6}$`));
  expect(
    isOperationNode(change.item) &&
      change.item.data.op.extensions?.['crossflow-auto-id'],
  ).toBe(true);
});
