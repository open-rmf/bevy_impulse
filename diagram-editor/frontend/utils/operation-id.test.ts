/** @jest-environment node */
import { applyNodeChanges, type NodeChange } from '@xyflow/react';
import { createDefaultEdge } from '../edges';
import { NodeManager } from '../node-manager';
import {
  createOperationNode,
  createScopeNode,
  type DiagramEditorNode,
  isOperationNode,
  TERMINATE_ID,
} from '../nodes';
import { saveState } from '../persist-state';
import type { DiagramElementMetadata } from '../types/api';
import { exportDiagram, exportTemplate } from './export-diagram';
import { loadDiagram, loadTemplate } from './load-diagram';
import { generateOperationId, updateOperationIds } from './operation-id';

const registry: DiagramElementMetadata = {
  messages: [],
  nodes: {},
  reverse_message_lookup: { result: [], split: [], unzip: [] },
  schemas: {},
  scripting: {},
  sections: {},
  trace_supported: false,
};
const op = {
  type: 'node',
  builder: 'add',
  next: { builtin: 'dispose' },
} as const;

function replace(node: DiagramEditorNode, nodes: DiagramEditorNode[]) {
  const changes: NodeChange<DiagramEditorNode>[] = [
    { type: 'replace', id: node.id, item: node },
  ];
  return applyNodeChanges(updateOperationIds(changes, nodes), nodes);
}

test('automatic names follow operation properties and retain their suffix', () => {
  const node = createOperationNode('', undefined, { x: 0, y: 0 }, op);
  const suffix = node.data.opId.slice(-6);
  const changed = {
    ...node,
    data: { ...node.data, op: { ...node.data.op, builder: 'subtract' } },
  };
  expect(replace(changed, [node])[0].data.opId).toBe(`node_subtract_${suffix}`);
  expect(generateOperationId({ ...op, builder: 'math::add' })).toMatch(
    /^node_math_add_[A-Z0-9]{6}$/,
  );
  expect(node.data.opId).toBe(`node_add_${suffix}`);
});

test('generated IDs avoid existing names, including other additions in the same change', () => {
  const first = createOperationNode('', undefined, { x: 0, y: 0 }, op);
  const second = { ...first, id: 'second', data: { ...first.data } };
  const changes: NodeChange<DiagramEditorNode>[] = [first, second].map(
    (item) => ({ type: 'add', item }),
  );
  const added = applyNodeChanges(updateOperationIds(changes, []), []);
  expect(added[0].data.opId).not.toBe(added[1].data.opId);
});

test.each([true, false, undefined])(
  'save/reload preserves automatic mode (%s) and renamed connections',
  async (automatic) => {
    const source = createOperationNode('', undefined, { x: 0, y: 0 }, op);
    const target = createOperationNode(
      '',
      undefined,
      { x: 0, y: 0 },
      { type: 'buffer' },
    );
    source.data.op.extensions = { 'crossflow-auto-id': automatic };
    const changed = {
      ...source,
      data: { ...source.data, op: { ...source.data.op, builder: 'subtract' } },
    };
    const nodes = replace(changed, [source, target]);
    const edges = [createDefaultEdge(source.id, null, target.id, null)];
    const diagram = exportDiagram(
      registry,
      new NodeManager(nodes),
      edges,
      {},
      {},
    );
    const id = nodes[0].data.opId as string;
    expect(diagram.ops[id]).toMatchObject({
      builder: 'subtract',
      next: target.data.opId,
    });
    expect(id.startsWith('node_subtract_')).toBe(automatic === true);
    await saveState(diagram, { nodes, edges });
    const loaded = await loadDiagram(JSON.parse(JSON.stringify(diagram)));
    const restored = loaded.graph.nodes.find((node) => node.id === source.id)!;
    expect(
      isOperationNode(restored) &&
        restored.data.op.extensions?.['crossflow-auto-id'],
    ).toBe(automatic);
    expect(loaded.graph.edges).toEqual(edges);
  },
);

test('automatic IDs remain automatic when a template is saved and reopened', () => {
  const node = createOperationNode('', undefined, { x: 0, y: 0 }, op);
  const template = exportTemplate(registry, new NodeManager([node]), []);
  const loaded = loadTemplate(JSON.parse(JSON.stringify(template)));
  const restored = loaded.nodes.find(isOperationNode)!;
  const changed = {
    ...restored,
    data: {
      ...restored.data,
      op: { ...restored.data.op, builder: 'subtract' },
    },
  };
  expect(
    replace(changed, loaded.nodes).find(isOperationNode)!.data.opId,
  ).toMatch(/^node_subtract_/);
});

test('renaming a scope preserves nested namespaces and builtin connections', () => {
  const [scope, start, terminate] = createScopeNode(
    '',
    undefined,
    { x: 0, y: 0 },
    {
      type: 'scope',
      start: { builtin: 'dispose' },
      next: { builtin: 'dispose' },
      ops: {},
    },
    'original',
  );
  const child = createOperationNode(':original', scope.id, { x: 0, y: 0 }, op);
  const nodes = replace(
    { ...scope, data: { ...scope.data, opId: 'renamed' } },
    [scope, start, terminate, child],
  );
  const edges = [
    createDefaultEdge(start.id, null, child.id, null),
    createDefaultEdge(child.id, null, terminate.id, null),
  ];
  const manager = new NodeManager(nodes);
  const diagram = exportDiagram(registry, manager, edges, {}, {});
  expect(diagram.ops.renamed).toMatchObject({
    start: child.data.opId,
    ops: { [child.data.opId]: { next: { builtin: 'terminate' } } },
  });
  expect(
    manager.getNodeFromNextOp(':renamed', { builtin: 'terminate' })!.id,
  ).toBe(terminate.id);
  expect(manager.getNodeFromNamespaceOpId(':renamed', TERMINATE_ID).id).toBe(
    terminate.id,
  );
});
