import {
  act,
  render,
  renderHook,
  screen,
  waitFor,
} from '@testing-library/react';
import { ReactFlowProvider } from '@xyflow/react';
import { of, Subject } from 'rxjs';
import {
  ConnectionCompatibilityProvider,
  useCompatibilityGraph,
  useDraggedConnectionCompatibility,
} from './connection-compatibility-provider';
import { ConnectionHintPanel } from './connection-hint-panel';
import { createDefaultEdge } from './edges';
import { NodeManager } from './node-manager';
import { createOperationNode } from './nodes';
import type { CompatibilityRequest, CompatibilityResponse } from './types/api';
import type { CompatibilityGraph } from './utils/compatibility';
import { ROOT_NAMESPACE } from './utils/namespace';

const mockApi = { checkCompatibility: jest.fn() };
type DragHandle = { nodeId: string; id: null; type: 'source' | 'target' };
let mockConnection: {
  inProgress: boolean;
  fromHandle: DragHandle | null;
  toHandle: DragHandle | null;
  to: { x: number; y: number };
};

jest.mock('./api-client-provider', () => ({ useApiClient: () => mockApi }));
jest.mock('@xyflow/react', () => ({
  ...jest.requireActual('@xyflow/react'),
  useConnection: () => mockConnection,
}));

function graph(nodeCount = 3): CompatibilityGraph {
  const nodes = Array.from({ length: nodeCount }, (_, index) =>
    createOperationNode(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      { type: 'fork_clone', next: [] },
      `operation_${index}`,
    ),
  );
  return {
    nodeManager: new NodeManager(nodes),
    edges: [createDefaultEdge(nodes[0].id, null, nodes[1].id, null)],
    templates: {},
    registry: {
      messages: [],
      nodes: {},
      sections: {},
      schemas: {},
      scripting: {},
      reverse_message_lookup: { result: [], split: [], unzip: [] },
      trace_supported: false,
    },
    diagramProperties: {},
  };
}

beforeEach(() => {
  mockConnection = {
    inProgress: false,
    fromHandle: null,
    toHandle: null,
    to: { x: 0, y: 0 },
  };
  mockApi.checkCompatibility.mockReset();
  mockApi.checkCompatibility.mockImplementation(
    (request: CompatibilityRequest) =>
      of({
        results: request.connections.map(({ id }) => ({
          id,
          status: 'incompatible',
          reason: 'wrong type',
        })),
      }),
  );
});

test('refreshes a 100-node graph in one batch and ignores layout and selection changes', async () => {
  const initial = graph(100);
  const nodes = initial.nodeManager.nodes;
  initial.edges = nodes
    .slice(1)
    .map((node, index) =>
      createDefaultEdge(nodes[index].id, null, node.id, null),
    );
  const { result, rerender } = renderHook(
    ({ value }) => useCompatibilityGraph(value),
    { initialProps: { value: initial } },
  );
  await waitFor(() => expect(result.current.edgeResults.size).toBe(99));
  expect(mockApi.checkCompatibility).toHaveBeenCalledTimes(1);
  const request: CompatibilityRequest =
    mockApi.checkCompatibility.mock.calls[0][0];
  expect(Object.keys(request.diagram.ops)).toHaveLength(100);
  expect(request.connections.map(({ id }) => id)).toEqual(
    initial.edges.map(({ id }) => id),
  );
  rerender({
    value: {
      ...initial,
      nodeManager: new NodeManager(
        nodes.map((node) => ({
          ...node,
          position: { x: 100, y: 200 },
          selected: true,
        })),
      ),
      edges: initial.edges.map((edge) => ({ ...edge, selected: true })),
    },
  });
  await act(async () => {
    await new Promise((resolve) => setTimeout(resolve, 180));
  });
  expect(mockApi.checkCompatibility).toHaveBeenCalledTimes(1);
});

test('invalidates cached results and discards late responses after changing the template', async () => {
  const stale = new Subject<CompatibilityResponse>();
  mockApi.checkCompatibility.mockReturnValueOnce(stale);
  const initial = graph();
  const { result, rerender } = renderHook(
    ({ value }) => useCompatibilityGraph(value),
    { initialProps: { value: initial } },
  );
  await waitFor(() =>
    expect(mockApi.checkCompatibility).toHaveBeenCalledTimes(1),
  );
  const connection = {
    source: initial.nodeManager.nodes[0].id,
    target: initial.nodeManager.nodes[2].id,
    sourceHandle: null,
    targetHandle: null,
  };
  await act(async () => {
    await Promise.all([
      result.current.checkConnection({ connection }),
      result.current.checkConnection({ connection }),
    ]);
  });
  expect(mockApi.checkCompatibility).toHaveBeenCalledTimes(2);
  const updated = { ...initial, templateId: 'edited' };
  mockApi.checkCompatibility.mockImplementation(
    (request: CompatibilityRequest) =>
      of({
        results: request.connections.map(({ id }) => ({
          id,
          status: 'compatible',
          reason: 'matches',
        })),
      }),
  );
  rerender({ value: updated });
  expect(result.current.edgeResults.size).toBe(0);
  await act(async () => {
    expect(await result.current.checkConnection({ connection })).toMatchObject({
      status: 'compatible',
    });
  });
  expect(mockApi.checkCompatibility).toHaveBeenCalledTimes(3);
  await waitFor(() =>
    expect(result.current.edgeResults.get(initial.edges[0].id)?.status).toBe(
      'compatible',
    ),
  );
  await act(async () => {
    stale.next({
      results: [
        { id: initial.edges[0].id, status: 'incompatible', reason: 'old' },
      ],
    });
  });
  expect(result.current.edgeResults.get(initial.edges[0].id)?.status).toBe(
    'compatible',
  );
});

function HandleFeedback({ nodeId }: { nodeId: string }) {
  const result = useDraggedConnectionCompatibility({
    otherNodeId: nodeId,
    otherHandleId: null,
    otherHandleType: 'target',
  });
  return <output data-testid={nodeId}>{result?.reason}</output>;
}

function DragFeedback({ value }: { value: CompatibilityGraph }) {
  const compatibility = useCompatibilityGraph(value);
  return (
    <ReactFlowProvider>
      <ConnectionCompatibilityProvider value={compatibility}>
        {value.nodeManager.nodes.map(({ id }) => (
          <HandleFeedback key={id} nodeId={id} />
        ))}
        <ConnectionHintPanel nodeManager={value.nodeManager} />
      </ConnectionCompatibilityProvider>
    </ReactFlowProvider>
  );
}

test('checks only the hovered pair and shares its pending result between handles and the panel', async () => {
  const initial = graph(100);
  initial.edges = [];
  const [source, firstTarget, secondTarget] = initial.nodeManager.nodes;
  mockConnection = {
    ...mockConnection,
    inProgress: true,
    fromHandle: { nodeId: source.id, id: null, type: 'source' },
  };
  const { rerender } = render(<DragFeedback value={initial} />);
  expect(mockApi.checkCompatibility).not.toHaveBeenCalled();

  const pending = new Subject<CompatibilityResponse>();
  mockApi.checkCompatibility.mockReturnValueOnce(pending);
  mockConnection = {
    ...mockConnection,
    toHandle: { nodeId: firstTarget.id, id: null, type: 'target' },
  };
  rerender(<DragFeedback value={initial} />);
  expect(mockApi.checkCompatibility).toHaveBeenCalledTimes(1);
  expect(mockApi.checkCompatibility.mock.calls[0][0].connections).toHaveLength(
    1,
  );

  mockConnection = { ...mockConnection, to: { x: 200, y: 300 } };
  rerender(<DragFeedback value={initial} />);
  expect(mockApi.checkCompatibility).toHaveBeenCalledTimes(1);
  await act(async () => {
    pending.next({
      results: [
        { id: 'preview', status: 'incompatible', reason: 'target mismatch' },
      ],
    });
  });
  expect(screen.getByTestId(firstTarget.id)).toHaveTextContent(
    'target mismatch',
  );
  expect(
    screen.getByText('target mismatch You can still create this connection.'),
  ).toBeInTheDocument();
  expect(screen.getByTestId(secondTarget.id)).toBeEmptyDOMElement();

  mockConnection = {
    ...mockConnection,
    toHandle: { nodeId: secondTarget.id, id: null, type: 'target' },
  };
  await act(async () => {
    rerender(<DragFeedback value={initial} />);
  });
  expect(mockApi.checkCompatibility).toHaveBeenCalledTimes(2);
  expect(screen.getByTestId(firstTarget.id)).toBeEmptyDOMElement();
  expect(screen.getByTestId(secondTarget.id)).toHaveTextContent('wrong type');
});
