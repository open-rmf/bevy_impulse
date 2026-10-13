import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from '@testing-library/react';
import { type ReactNode, useEffect } from 'react';
import { of } from 'rxjs';
import { ApiClient } from '../api-client';
import { ApiClientProvider } from '../api-client-provider';
import {
  DiagramPropertiesProvider,
  useDiagramProperties,
} from '../diagram-properties-provider';
import {
  DiagramSidePanelProvider,
  useDiagramSidePanel,
} from '../diagram-side-panel-controller';
import { NodeManager, NodeManagerProvider } from '../node-manager';
import { NotificationProvider } from '../notification-provider';
import { RegistryProvider } from '../registry-provider';
import {
  TransientEditorDraftProvider,
  useTransientEditorDrafts,
} from '../transient-editor-drafts';
import type { DiagramElementMetadata } from '../types/api';
import {
  ScriptEnvironmentWorkspace,
  type ScriptNodeEnvironmentBinding,
} from './script-environment-workspace';

jest.mock('@uiw/react-codemirror', () => ({
  __esModule: true,
  default: ({
    value,
    onChange,
  }: {
    value: string;
    onChange: (value: string) => void;
  }) => (
    <textarea
      aria-label="Script editor"
      value={value}
      onChange={(event) => onChange(event.target.value)}
    />
  ),
}));

const testRegistry: DiagramElementMetadata = {
  messages: [],
  nodes: {},
  schemas: {},
  sections: {},
  trace_supported: false,
  reverse_message_lookup: {
    result: [],
    split: [],
    unzip: [],
  },
  scripting: {
    'generic-script-builder': {
      language: 'bash',
      interpreter: '/bin/bash',
      display_text: 'Bash Script',
      description: 'Run bash scripts',
      config_schema: {
        properties: {
          script_content: {
            type: 'string',
          },
        },
      },
      config_examples: [],
    },
    'process-bound-python': {
      language: 'python',
      interpreter: 'python3',
      display_text: 'Python',
      description: 'Process-bound Python interpreter',
      config_schema: {
        properties: {
          ownership: {
            enum: ['shared', 'persistent', 'isolated'],
          },
          script: {
            type: 'string',
          },
        },
      },
      config_examples: [
        {
          name: 'Shared Python Environment',
          description: 'Reused environment with shared state',
          config: {
            ownership: 'shared',
            script: 'print("Hello from example")',
          },
          run: 'execute',
        },
      ],
    },
  },
};

function OpenWorkspace() {
  const { openEnvironments } = useDiagramSidePanel();

  useEffect(() => {
    openEnvironments({ create: true });
  }, [openEnvironments]);

  return <ScriptEnvironmentWorkspace />;
}

function SeededWorkspace({
  scriptNodeBinding,
}: {
  scriptNodeBinding?: ScriptNodeEnvironmentBinding;
} = {}) {
  const [properties, setProperties] = useDiagramProperties();
  const { openEnvironments } = useDiagramSidePanel();

  useEffect(() => {
    setProperties({
      script_environments: {
        analysis: {
          builder: 'process-bound-python',
          config: {
            ownership: 'persistent',
            script: 'def run(): pass',
          },
        },
      },
    });
    openEnvironments({ environmentName: 'analysis' });
  }, [openEnvironments, setProperties]);

  return (
    <>
      <output data-testid="highlight">
        {properties.highlightedEnvironment || 'none'}
      </output>
      <ScriptEnvironmentWorkspace scriptNodeBinding={scriptNodeBinding} />
    </>
  );
}

function BoundEmptyWorkspace({
  assignEnvironment,
}: {
  assignEnvironment: (environmentName: string) => void;
}) {
  return (
    <ScriptEnvironmentWorkspace scriptNodeBinding={{ assignEnvironment }} />
  );
}

function MissingWorkspace() {
  const { openEnvironments } = useDiagramSidePanel();

  useEffect(() => {
    openEnvironments({ environmentName: 'missing-env' });
  }, [openEnvironments]);

  return <ScriptEnvironmentWorkspace />;
}

function PanelStateProbe() {
  const { state } = useDiagramSidePanel();
  return <output data-testid="panel-state">{JSON.stringify(state)}</output>;
}

function renderWorkspace(
  ui: ReactNode,
  registry: DiagramElementMetadata = testRegistry,
) {
  const apiClient = new ApiClient();
  jest.spyOn(apiClient, 'getRegistry').mockReturnValue(of(registry));

  return render(
    <ApiClientProvider value={apiClient}>
      <RegistryProvider>
        <NodeManagerProvider value={new NodeManager([])}>
          <DiagramPropertiesProvider>
            <TransientEditorDraftProvider>
              <DiagramSidePanelProvider>
                <NotificationProvider>{ui}</NotificationProvider>
              </DiagramSidePanelProvider>
            </TransientEditorDraftProvider>
          </DiagramPropertiesProvider>
        </NodeManagerProvider>
      </RegistryProvider>
    </ApiClientProvider>,
  );
}

async function startEnvironmentCreationFromSelector() {
  fireEvent.mouseDown(
    await screen.findByRole('combobox', { name: 'Select Environment' }),
  );
  fireEvent.click(
    screen.getByRole('option', { name: /Create new environment/ }),
  );
}

describe('ScriptEnvironmentWorkspace', () => {
  test('defaults new environments to process-bound-python', async () => {
    renderWorkspace(<OpenWorkspace />);

    expect(
      await screen.findByRole('combobox', { name: 'Builder' }),
    ).toHaveTextContent('process-bound-python');
    expect(screen.getByLabelText('Environment Ownership')).toBeInTheDocument();
  });

  test('keeps the builder selectable for future runtimes', async () => {
    renderWorkspace(<OpenWorkspace />);

    const builder = await screen.findByRole('combobox', { name: 'Builder' });
    fireEvent.mouseDown(builder);
    fireEvent.click(
      screen.getByRole('option', { name: 'generic-script-builder' }),
    );

    await waitFor(() => {
      expect(
        screen.getByLabelText('Builder Config (JSON)'),
      ).toBeInTheDocument();
    });
    const config = screen.getByLabelText('Builder Config (JSON)');
    fireEvent.change(config, {
      target: { value: '{"script_content":"first"}' },
    });
    fireEvent.mouseDown(screen.getByLabelText('Code Field'));
    fireEvent.click(screen.getByRole('option', { name: 'script_content' }));
    expect(scriptEditor()).toHaveValue('first');
    changeScript('second');
    expect(JSON.parse((config as HTMLTextAreaElement).value)).toEqual({
      script_content: 'second',
    });
  });

  test('selecting an environment does not highlight its nodes', async () => {
    renderWorkspace(<SeededWorkspace />);

    expect(
      await screen.findByRole('heading', { name: 'analysis' }),
    ).toBeInTheDocument();
    expect(screen.getByTestId('highlight')).toHaveTextContent('none');
  });

  test('the eye button explicitly highlights the selected environment', async () => {
    renderWorkspace(<SeededWorkspace />);

    fireEvent.click(await screen.findByRole('button', { name: 'Show nodes' }));

    expect(screen.getByTestId('highlight')).toHaveTextContent('analysis');
    expect(
      screen.getByRole('button', { name: 'Hide nodes' }),
    ).toBeInTheDocument();
  });

  test('uses the script node environment instead of a second selector', async () => {
    renderWorkspace(
      <SeededWorkspace
        scriptNodeBinding={{
          environmentName: 'analysis',
          assignEnvironment: jest.fn(),
        }}
      />,
    );

    expect(
      await screen.findByRole('heading', { name: 'analysis' }),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole('combobox', { name: 'Select Environment' }),
    ).not.toBeInTheDocument();
  });

  test('keeps the environment selector in standalone mode', async () => {
    renderWorkspace(<SeededWorkspace />);

    expect(
      await screen.findByRole('combobox', { name: 'Select Environment' }),
    ).toBeInTheDocument();
  });

  test('starts creation from the standalone environment selector', async () => {
    renderWorkspace(
      <>
        <ScriptEnvironmentWorkspace />
        <PanelStateProbe />
      </>,
    );

    await startEnvironmentCreationFromSelector();

    expect(screen.getByTestId('panel-state')).toHaveTextContent(
      '"mode":"create"',
    );
    expect(
      screen.queryByRole('button', { name: 'Create new environment' }),
    ).not.toBeInTheDocument();
  });

  test('keeps the selector visible while assigning creation to a newly added script', async () => {
    const assignEnvironment = jest.fn();
    renderWorkspace(
      <ScriptEnvironmentWorkspace
        scriptNodeBinding={{
          assignEnvironment,
          nodeControlsSelection: false,
        }}
      />,
    );

    await startEnvironmentCreationFromSelector();
    fireEvent.change(screen.getByLabelText('Environment Name'), {
      target: { value: 'analysis' },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));

    await waitFor(() => {
      expect(assignEnvironment).toHaveBeenCalledWith('analysis');
    });
  });

  test('offers environment creation when the bound node has no choices', async () => {
    renderWorkspace(<BoundEmptyWorkspace assignEnvironment={jest.fn()} />);

    expect(
      await screen.findByRole('button', { name: 'Create environment' }),
    ).toBeInTheDocument();
    expect(
      screen.getAllByRole('button', { name: /create.*environment/i }),
    ).toHaveLength(1);
    expect(
      screen.queryByRole('combobox', { name: 'Select Environment' }),
    ).not.toBeInTheDocument();
  });

  test('directs an unassigned node to its own selector when choices exist', async () => {
    renderWorkspace(
      <SeededWorkspace scriptNodeBinding={{ assignEnvironment: jest.fn() }} />,
    );

    expect(
      await screen.findByText(
        'Select an environment in the script node editor.',
      ),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole('combobox', { name: 'Select Environment' }),
    ).not.toBeInTheDocument();
  });

  test('offers recovery when a node references a missing environment', async () => {
    renderWorkspace(<MissingWorkspace />);

    expect(
      await screen.findByText('Environment “missing-env” is not defined.'),
    ).toBeInTheDocument();
    expect(
      screen.getByRole('button', { name: 'Create environment' }),
    ).toBeInTheDocument();
  });

  test('save returns the drawer to normal width', async () => {
    renderWorkspace(
      <>
        <OpenWorkspace />
        <PanelStateProbe />
      </>,
    );

    fireEvent.change(await screen.findByLabelText('Environment Name'), {
      target: { value: 'analysis' },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));

    await waitFor(() => {
      expect(screen.getByTestId('panel-state')).toHaveTextContent(
        '"expanded":false',
      );
      expect(screen.getByTestId('panel-state')).toHaveTextContent(
        '"mode":"view"',
      );
    });
  });

  test('assigns a created environment to the bound script node', async () => {
    const assignEnvironment = jest.fn();
    renderWorkspace(
      <BoundEmptyWorkspace assignEnvironment={assignEnvironment} />,
    );

    fireEvent.click(
      await screen.findByRole('button', { name: 'Create environment' }),
    );
    fireEvent.change(screen.getByLabelText('Environment Name'), {
      target: { value: 'analysis' },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));

    await waitFor(() => {
      expect(assignEnvironment).toHaveBeenCalledWith('analysis');
      expect(assignEnvironment).toHaveBeenCalledTimes(1);
    });
  });

  test('does not assign an environment when creation is cancelled', async () => {
    const assignEnvironment = jest.fn();
    renderWorkspace(
      <BoundEmptyWorkspace assignEnvironment={assignEnvironment} />,
    );

    fireEvent.click(
      await screen.findByRole('button', { name: 'Create environment' }),
    );
    fireEvent.change(screen.getByLabelText('Environment Name'), {
      target: { value: 'analysis' },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));

    expect(assignEnvironment).not.toHaveBeenCalled();
  });

  test('selecting an existing environment from create mode switches to edit mode', async () => {
    renderWorkspace(
      <>
        <SeededWorkspace />
        <PanelStateProbe />
      </>,
    );

    await startEnvironmentCreationFromSelector();
    fireEvent.click(screen.getByRole('button', { name: 'analysis' }));

    await waitFor(() => {
      expect(screen.getByTestId('panel-state')).toHaveTextContent(
        '"mode":"edit"',
      );
      expect(screen.getByLabelText('Environment Name')).toBeDisabled();
    });
  });

  test('starting another environment in create mode resets the draft', async () => {
    renderWorkspace(<SeededWorkspace />);

    await startEnvironmentCreationFromSelector();
    const environmentName = screen.getByLabelText('Environment Name');
    fireEvent.change(environmentName, {
      target: { value: 'unfinished-draft' },
    });

    fireEvent.click(screen.getByRole('button', { name: /New environment/ }));
    fireEvent.click(
      within(screen.getByRole('dialog')).getByRole('button', {
        name: 'Discard',
      }),
    );

    await waitFor(() => {
      expect(environmentName).toHaveValue('');
    });
  });

  test('disables creation when no builders are registered', async () => {
    renderWorkspace(<OpenWorkspace />, {
      ...testRegistry,
      scripting: {},
    });

    expect(
      await screen.findByText('No script environment builders are available.'),
    ).toBeInTheDocument();
  });
});

let panel: ReturnType<typeof useDiagramSidePanel>;
let drafts: ReturnType<typeof useTransientEditorDrafts>;
let properties: ReturnType<typeof useDiagramProperties>[0];
function SessionProbe() {
  panel = useDiagramSidePanel();
  drafts = useTransientEditorDrafts();
  [properties] = useDiagramProperties();
  return null;
}

function click(name: string, root: HTMLElement = document.body) {
  fireEvent.click(within(root).getByRole('button', { name }));
}
const scriptEditor = () =>
  screen.getByRole('textbox', { name: 'Script editor' });
const changeScript = (value: string) =>
  fireEvent.change(scriptEditor(), { target: { value } });
async function editEnvironment() {
  renderWorkspace(
    <>
      <SeededWorkspace />
      <SessionProbe />
    </>,
  );
  fireEvent.click(
    await screen.findByRole('button', { name: 'Edit environment' }),
  );
}

test('only actual changes block export; reverting or saving clears the draft', async () => {
  await editEnvironment();
  expect(drafts.hasUncommittedBuffers).toBe(false);
  changeScript('changed code');
  expect(drafts.hasUncommittedBuffers).toBe(true);
  expect(properties.script_environments?.analysis.config).toMatchObject({
    script: 'def run(): pass',
  });
  changeScript('def run(): pass');
  expect(drafts.hasUncommittedBuffers).toBe(false);
  changeScript('saved code');
  click('Save');
  expect(drafts.drafts.scriptEnvironment).toBeUndefined();
  expect(properties.script_environments?.analysis.config).toMatchObject({
    script: 'saved code',
  });
  await startEnvironmentCreationFromSelector();
  expect(drafts.hasUncommittedBuffers).toBe(false);
});

test('Cancel asks before discarding; cancelling or Escape preserves the changes', async () => {
  await editEnvironment();
  changeScript('unsaved');
  click('Cancel');
  const dialog = screen.getByRole('dialog');
  expect(dialog).toHaveTextContent('Discard environment changes?');
  click('Cancel', dialog);
  expect(
    await screen.findByRole('textbox', { name: 'Script editor' }),
  ).toHaveValue('unsaved');
  click('Cancel');
  fireEvent.keyDown(screen.getByRole('dialog'), {
    key: 'Escape',
    code: 'Escape',
  });
  expect(
    await screen.findByRole('textbox', { name: 'Script editor' }),
  ).toHaveValue('unsaved');
  click('Cancel');
  click('Discard', screen.getByRole('dialog'));
  expect(drafts.drafts.scriptEnvironment).toBeUndefined();
  expect(properties.script_environments?.analysis.config).toMatchObject({
    script: 'def run(): pass',
  });
});

test('navigation can be cancelled, saved, or discarded without bypassing validation', async () => {
  await editEnvironment();
  changeScript('updated');
  for (const navigate of [
    panel.close,
    () => panel.showTab('run'),
    () => panel.toggleTab('environments'),
    () => panel.openEnvironments({ create: true }),
  ]) {
    act(navigate);
    const dialog = screen.getByRole('dialog');
    expect(dialog).toHaveTextContent('Save environment changes?');
    click('Cancel', dialog);
    expect(panel.state).toMatchObject({ open: true, tab: 'environments' });
    expect(
      await screen.findByRole('textbox', { name: 'Script editor' }),
    ).toHaveValue('updated');
  }
  act(panel.close);
  click('Save & Continue', screen.getByRole('dialog'));
  expect(panel.state.open).toBe(false);
  expect(properties.script_environments?.analysis.config).toMatchObject({
    script: 'updated',
  });
  act(() => panel.openEnvironments({ create: true }));
  fireEvent.change(screen.getByLabelText('Environment Name'), {
    target: { value: 'analysis' },
  });
  act(() => panel.showTab('run'));
  expect(
    within(screen.getByRole('dialog')).getByRole('button', {
      name: 'Save & Continue',
    }),
  ).toBeDisabled();
  click('Discard', screen.getByRole('dialog'));
  expect(panel.state.tab).toBe('run');
  expect(drafts.drafts.scriptEnvironment).toBeUndefined();
});

test('popup edits stay unapplied and closing it returns the same editor to the panel', async () => {
  await editEnvironment();
  const editor = scriptEditor();
  const frame = document.createElement('iframe');
  document.body.appendChild(frame);
  const popup = frame.contentWindow!;
  const open = jest.spyOn(window, 'open').mockReturnValue(popup);
  const close = jest.spyOn(popup, 'close').mockImplementation(() => {});
  click('Open environment in separate window');
  expect(
    within(popup.document.body).getByRole('textbox', { name: 'Script editor' }),
  ).toBe(editor);
  fireEvent.change(editor, { target: { value: 'edited in window' } });
  act(panel.close);
  expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  act(() => popup.dispatchEvent(new Event('pagehide')));
  await waitFor(() => expect(scriptEditor()).toBe(editor));
  expect(editor).toHaveValue('edited in window');
  expect(panel.state).toMatchObject({ open: true, tab: 'environments' });
  expect(properties.script_environments?.analysis.config).toMatchObject({
    script: 'def run(): pass',
  });
  open.mockRestore();
  close.mockRestore();
  frame.remove();
});
