import {
  act,
  fireEvent,
  render,
  renderHook,
  screen,
} from '@testing-library/react';
import {
  DiagramSidePanelProvider,
  useDiagramSidePanel,
} from './diagram-side-panel-controller';

import { getDiagramSidePanelWidth } from './diagram-side-panel-layout';

function Harness() {
  const panel = useDiagramSidePanel();

  return (
    <>
      <output data-testid="state">{JSON.stringify(panel.state)}</output>
      <button type="button" onClick={() => panel.toggleTab('run')}>
        toggle run
      </button>
      <button
        type="button"
        onClick={() => panel.openEnvironments({ environmentName: 'analysis' })}
      >
        open analysis
      </button>
      <button type="button" onClick={() => panel.startEnvironmentCreate()}>
        create environment
      </button>
      <button type="button" onClick={() => panel.finishEnvironmentEdit()}>
        finish edit
      </button>
    </>
  );
}

function renderHarness() {
  return render(
    <DiagramSidePanelProvider>
      <Harness />
    </DiagramSidePanelProvider>,
  );
}

describe('DiagramSidePanelProvider', () => {
  test.each(['startEnvironmentCreate', 'startEnvironmentEdit'] as const)(
    '%s restores automatic widening after manual resizing',
    (action) => {
      const { result } = renderHook(useDiagramSidePanel, {
        wrapper: DiagramSidePanelProvider,
      });
      act(() => result.current.setWidth(500));
      act(() => result.current[action]());
      expect(getDiagramSidePanelWidth(1440, result.current.state)).toBe(900);
    },
  );

  test('opens and selects an environment through one shared action', () => {
    renderHarness();

    fireEvent.click(screen.getByRole('button', { name: 'open analysis' }));

    expect(screen.getByTestId('state')).toHaveTextContent(
      '"tab":"environments"',
    );
    expect(screen.getByTestId('state')).toHaveTextContent(
      '"selectedEnvironmentName":"analysis"',
    );
    expect(screen.getByTestId('state')).toHaveTextContent('"open":true');
  });

  test('create mode expands and finishing edit collapses the drawer', () => {
    renderHarness();

    fireEvent.click(screen.getByRole('button', { name: 'create environment' }));
    expect(screen.getByTestId('state')).toHaveTextContent('"mode":"create"');
    expect(screen.getByTestId('state')).toHaveTextContent('"expanded":true');

    fireEvent.click(screen.getByRole('button', { name: 'finish edit' }));
    expect(screen.getByTestId('state')).toHaveTextContent('"mode":"view"');
    expect(screen.getByTestId('state')).toHaveTextContent('"expanded":false');
  });

  test('switching away from environments resets the editor state', () => {
    renderHarness();

    fireEvent.click(screen.getByRole('button', { name: 'create environment' }));
    fireEvent.click(screen.getByRole('button', { name: 'toggle run' }));

    expect(screen.getByTestId('state')).toHaveTextContent('"tab":"run"');
    expect(screen.getByTestId('state')).toHaveTextContent('"mode":"view"');
    expect(screen.getByTestId('state')).toHaveTextContent('"expanded":false');
  });
});
