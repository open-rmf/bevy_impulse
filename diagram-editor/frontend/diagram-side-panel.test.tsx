import { createTheme, ThemeProvider } from '@mui/material';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { useEffect } from 'react';
import { DiagramPropertiesProvider } from './diagram-properties-provider';
import DiagramSidePanel from './diagram-side-panel';
import {
  DiagramSidePanelProvider,
  useDiagramSidePanel,
} from './diagram-side-panel-controller';

jest.mock('./run-button', () => ({
  RunPanel: () => null,
}));

jest.mock('./forms/script-environment-workspace', () => ({
  ScriptEnvironmentWorkspace: ({
    scriptNodeBinding,
  }: {
    scriptNodeBinding?: { environmentName?: string };
  }) => (
    <output data-testid="workspace-binding">
      {scriptNodeBinding?.environmentName}
    </output>
  ),
}));

function OpenCreateMode() {
  const { openEnvironments } = useDiagramSidePanel();

  useEffect(() => {
    openEnvironments({ create: true });
  }, [openEnvironments]);

  return null;
}

function renderSidePanel({
  create = false,
  scriptNodeBinding,
}: {
  create?: boolean;
  scriptNodeBinding?: {
    environmentName?: string;
    assignEnvironment: (environmentName: string) => void;
  };
} = {}) {
  return render(
    <ThemeProvider theme={createTheme({ palette: { mode: 'dark' } })}>
      <DiagramPropertiesProvider>
        <DiagramSidePanelProvider>
          {create && <OpenCreateMode />}
          <DiagramSidePanel
            runRequestJson=""
            onRunRequestJsonChange={jest.fn()}
            scriptNodeBinding={scriptNodeBinding}
          />
        </DiagramSidePanelProvider>
      </DiagramPropertiesProvider>
    </ThemeProvider>,
  );
}

describe('DiagramSidePanel', () => {
  beforeEach(() => {
    Object.defineProperty(window, 'innerWidth', {
      configurable: true,
      writable: true,
      value: 1440,
    });
  });

  test('renders Environments beside Properties and Run', () => {
    renderSidePanel();

    expect(
      screen.getByRole('tab', { name: /Properties/i }),
    ).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: /Run/i })).toBeInTheDocument();
    expect(
      screen.getByRole('tab', { name: /Environments/i }),
    ).toBeInTheDocument();
  });

  test('uses an animatable pixel width for the normal drawer', () => {
    renderSidePanel();

    const drawer = screen.getByTestId('diagram-side-panel-paper');
    const drawerPaper = document.querySelector(
      '.MuiDrawer-paper',
    ) as HTMLElement;

    expect(drawer.style.width).toBe('480px');
    expect(drawerPaper.style.width).toBe('480px');
    expect(drawerPaper.style.transition).toContain('width');

    const edge = drawerPaper.firstElementChild as HTMLElement;
    edge.setPointerCapture = jest.fn();
    for (const type of ['pointerdown', 'pointermove']) {
      fireEvent(
        edge,
        new MouseEvent(type, { bubbles: true, button: 0, clientX: 740 }),
      );
    }
    expect(drawer.style.width).toBe('700px');
  });

  test('animates the drawer to its expanded pixel width', async () => {
    renderSidePanel({ create: true });

    const drawer = await screen.findByTestId('diagram-side-panel-paper');
    const drawerPaper = document.querySelector(
      '.MuiDrawer-paper',
    ) as HTMLElement;

    await waitFor(() => {
      expect(drawer).toHaveAttribute('data-expanded', 'true');
      expect(drawer.style.width).toBe('900px');
      expect(drawerPaper.style.width).toBe('900px');
      expect(drawerPaper.style.transition).toContain('width');
    });
  });

  test('forwards the active script node environment to the workspace', () => {
    renderSidePanel({
      scriptNodeBinding: {
        environmentName: 'analysis',
        assignEnvironment: jest.fn(),
      },
    });

    expect(screen.getByTestId('workspace-binding')).toHaveTextContent(
      'analysis',
    );
  });
});
