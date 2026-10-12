import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';
import { useState } from 'react';
import { Subject } from 'rxjs';
import type { BaseApiClient } from './api-client';
import { ApiClientProvider } from './api-client-provider';
import { InteractionVisualizationProvider } from './interaction-visualization-provider';
import { RunPanel } from './run-button';
import type { Diagram, InteractionSessionMessage } from './types/api';

const mockDiagram: Diagram = {
  version: '0.1.0',
  start: 'source',
  ops: {
    source: { type: 'node', builder: 'echo', next: { builtin: 'terminate' } },
  },
};
const mockExportDiagram = jest.fn((..._args: unknown[]) => mockDiagram);

jest.mock('./utils/export-diagram', () => ({
  exportDiagram: (...args: unknown[]) => mockExportDiagram(...args),
}));
jest.mock('./node-manager', () => ({ useNodeManager: () => ({}) }));
jest.mock('./use-edges', () => ({ useEdges: () => [] }));
jest.mock('./templates-provider', () => ({ useTemplates: () => [{}] }));
jest.mock('./registry-provider', () => ({ useRegistry: () => ({}) }));
jest.mock('./diagram-properties-provider', () => ({
  useDiagramProperties: () => [{}],
}));
jest.mock('./nodes', () => ({
  MaterialSymbol: ({ symbol }: { symbol: string }) => <span>{symbol}</span>,
}));

const visualization = {
  activeNodeIds: new Set<string>(),
  visitedNodeIds: new Set<string>(),
  clearInteractionVisualization: jest.fn(),
  markInteractionFinished: jest.fn(),
  markInteractionOperationFinished: jest.fn(),
  markInteractionOperationStarted: jest.fn(),
};

function TestRunPanel({
  apiClient,
  showPanel = true,
}: {
  apiClient: BaseApiClient;
  showPanel?: boolean;
}) {
  const [isWorkflowRunning, setWorkflowRunning] = useState(false);
  return (
    <ApiClientProvider value={apiClient}>
      <InteractionVisualizationProvider
        value={{ ...visualization, isWorkflowRunning, setWorkflowRunning }}
      >
        <output data-testid="running">{String(isWorkflowRunning)}</output>
        {showPanel && (
          <RunPanel
            requestJsonString="{}"
            onRequestJsonStringChange={() => {}}
          />
        )}
      </InteractionVisualizationProvider>
    </ApiClientProvider>
  );
}

function apiFixture() {
  const responses = new Subject<unknown>();
  const messages = new Subject<InteractionSessionMessage>();
  const session = { interactionMessages$: messages, close: jest.fn() };
  const apiClient = {
    getRegistry: jest.fn(),
    checkCompatibility: jest.fn(),
    postRunWorkflow: jest.fn(() => responses),
    wsInteractWithWorkflow: jest.fn().mockResolvedValue(session),
  };
  return { apiClient, responses, messages, session };
}

beforeEach(() => {
  jest.clearAllMocks();
  mockExportDiagram.mockImplementation(() => mockDiagram);
});

async function startRun(showProgress: boolean) {
  if (!showProgress) {
    fireEvent.click(screen.getByRole('checkbox', { name: 'Show progress' }));
  }
  fireEvent.click(screen.getByRole('button', { name: /Run/ }));
  await waitFor(() =>
    expect(screen.getByTestId('running')).toHaveTextContent('true'),
  );
}

test.each([false, true])(
  'shares running state through completion and panel close (progress: %s)',
  async (showProgress) => {
    const { apiClient, responses, messages } = apiFixture();
    const view = render(<TestRunPanel apiClient={apiClient} />);
    expect(screen.getByTestId('running')).toHaveTextContent('false');
    await startRun(showProgress);
    act(() => {
      if (showProgress) messages.next({ type: 'finish', ok: 42 });
      else responses.next(42);
    });
    expect(screen.getByTestId('running')).toHaveTextContent('false');
    fireEvent.click(screen.getByRole('button', { name: /Run/ }));
    await waitFor(() =>
      expect(screen.getByTestId('running')).toHaveTextContent('true'),
    );
    view.rerender(<TestRunPanel apiClient={apiClient} showPanel={false} />);
    expect(screen.getByTestId('running')).toHaveTextContent('false');
  },
);
