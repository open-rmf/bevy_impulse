import { indentWithTab } from '@codemirror/commands';
import { indentUnit } from '@codemirror/language';
import { EditorView, keymap } from '@codemirror/view';
import {
  Alert,
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  IconButton,
  List,
  ListItemButton,
  ListItemText,
  MenuItem,
  Paper,
  Stack,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material';
import CodeMirror, { type ReactCodeMirrorRef } from '@uiw/react-codemirror';
import equal from 'fast-deep-equal';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useDiagramProperties } from '../diagram-properties-provider';
import { useDiagramSidePanel } from '../diagram-side-panel-controller';
import {
  isScriptEnvironmentDraftDirty,
  type ScriptEnvironmentEditorDraft,
} from '../draft-workspace';
import { useNodeManager } from '../node-manager';
import { MaterialSymbol } from '../nodes';
import { useNotification } from '../notification-provider';
import { useRegistry } from '../registry-provider';
import { useTransientEditorDrafts } from '../transient-editor-drafts';
import type { DiagramElementMetadata } from '../types/api';
import { EnvironmentWindow } from './environment-window';
import { ScriptEnvironmentPanel } from './script-environment-panel';
import { scriptEnvironmentPlugins } from './script-environments/registry';

const CREATE_ENVIRONMENT_VALUE = '__create_script_environment__';

function objectConfig(value: unknown): Record<string, unknown> {
  return typeof value === 'object' && value !== null
    ? (value as Record<string, unknown>)
    : {};
}

export interface ScriptNodeEnvironmentBinding {
  nodeId?: string;
  environmentName?: string;
  assignEnvironment: (environmentName: string) => void;
  nodeControlsSelection?: boolean;
}

export interface ScriptEnvironmentWorkspaceProps {
  scriptNodeBinding?: ScriptNodeEnvironmentBinding;
}

export function ScriptEnvironmentWorkspace({
  scriptNodeBinding,
}: ScriptEnvironmentWorkspaceProps) {
  const [diagramProperties, setDiagramProperties] = useDiagramProperties();
  const nodeManager = useNodeManager();
  const showNotification = useNotification();
  const rawRegistry = useRegistry();
  const registry = rawRegistry as unknown as DiagramElementMetadata;
  const environments = diagramProperties.script_environments || {};
  const {
    state: { selectedEnvironmentName, mode: panelMode, expanded, width },
    selectEnvironment,
    startEnvironmentCreate,
    startEnvironmentEdit,
    finishEnvironmentEdit,
    setExpanded,
    setWidth,
    showTab,
    setEnvironmentLeaveGuard,
    requestEnvironmentLeave,
  } = useDiagramSidePanel();
  const nodeControlsSelection =
    !!scriptNodeBinding && scriptNodeBinding.nodeControlsSelection !== false;
  const activeEnvironmentName = nodeControlsSelection
    ? scriptNodeBinding.environmentName
    : selectedEnvironmentName;

  const registeredBuilders = useMemo(
    () => (registry.scripting ? Object.keys(registry.scripting) : []),
    [registry],
  );
  const defaultBuilder = registeredBuilders.includes('process-bound-python')
    ? 'process-bound-python'
    : registeredBuilders[0] || '';

  const {
    drafts: { scriptEnvironment: draft },
    setScriptEnvironmentDraft,
  } = useTransientEditorDrafts();
  const {
    envName = '',
    builder = '',
    config = '{}',
    selectedCodeField = '',
  } = draft || {};
  const mode = draft?.mode || 'view';
  const [configError, setConfigError] = useState<string | null>(null);
  const nameError =
    mode === 'create' && environments[envName] ? 'Duplicated name' : null;
  const [editorWindow, setEditorWindow] = useState<Window | null>(null);
  const codeEditor = useRef<ReactCodeMirrorRef>(null);
  const dockedLayout = useRef({ expanded, width });
  const returnToPanel = useCallback(() => {
    setEditorWindow(null);
    showTab('environments');
    setExpanded(dockedLayout.current.expanded);
    if (dockedLayout.current.width !== undefined)
      setWidth(dockedLayout.current.width);
  }, [showTab, setExpanded, setWidth]);
  useEffect(() => {
    const view = codeEditor.current?.view;
    if (view) {
      view.setRoot(editorWindow?.document || document);
      view.requestMeasure();
    }
  }, [editorWindow]);
  useEffect(() => {
    if (!editorWindow) return;
    const close = () => editorWindow.close();
    editorWindow.addEventListener('pagehide', returnToPanel);
    window.addEventListener('pagehide', close);
    return () => {
      editorWindow.removeEventListener('pagehide', returnToPanel);
      window.removeEventListener('pagehide', close);
      close();
    };
  }, [editorWindow, returnToPanel]);
  useEffect(() => {
    if (!draft) setEditorWindow(null);
  }, [draft]);
  const [pendingLeave, setPendingLeave] = useState<{
    proceed: () => void;
    discardOnly: boolean;
  } | null>(null);
  const endEditing = useCallback(() => {
    setScriptEnvironmentDraft(undefined);
    finishEnvironmentEdit();
  }, [setScriptEnvironmentDraft, finishEnvironmentEdit]);
  useEffect(() => {
    if (!draft) return;
    setEnvironmentLeaveGuard((proceed, hidingOnly) => {
      if (editorWindow && hidingOnly) {
        proceed();
        return;
      }
      if (isScriptEnvironmentDraftDirty(draft))
        setPendingLeave({ proceed, discardOnly: false });
      else {
        endEditing();
        proceed();
      }
    });
    return () => setEnvironmentLeaveGuard(null);
  }, [draft, editorWindow, endEditing, setEnvironmentLeaveGuard]);
  const updateDraft = useCallback(
    (patch: Partial<ScriptEnvironmentEditorDraft>) => {
      setScriptEnvironmentDraft((previous) =>
        previous ? { ...previous, ...patch } : undefined,
      );
    },
    [setScriptEnvironmentDraft],
  );
  const captureEditor = (view: EditorView) => {
    const { anchor, head } = view.state.selection.main;
    const { scrollTop, scrollLeft } = view.scrollDOM;
    const editor = { anchor, head, scrollTop, scrollLeft };
    setScriptEnvironmentDraft((previous) =>
      !previous || equal(previous.editor, editor)
        ? previous
        : { ...previous, editor },
    );
  };
  useEffect(() => {
    if (
      !editorWindow &&
      draft &&
      (draft.isExpanded !== expanded || draft.width !== width)
    )
      updateDraft({ isExpanded: expanded, width });
  }, [draft, editorWindow, expanded, width, updateDraft]);
  const plugin = scriptEnvironmentPlugins[builder];
  const activeCodeField = plugin?.defaultCodeField || selectedCodeField;

  const builderDraft = useCallback(
    (builder: string) => ({
      builder,
      selectedCodeField:
        scriptEnvironmentPlugins[builder]?.defaultCodeField || '',
      config: JSON.stringify(
        registry.scripting?.[builder]?.config_examples?.[0]?.config ??
          JSON.parse(
            scriptEnvironmentPlugins[builder]?.bootstrapConfig?.() || '{}',
          ),
        null,
        2,
      ),
    }),
    [registry.scripting],
  );

  const beginEditing = useCallback(
    (name?: string) => {
      const environment = name ? environments[name] : undefined;
      const values = builderDraft(environment?.builder || defaultBuilder);
      if (environment)
        values.config = JSON.stringify(
          objectConfig(environment.config),
          null,
          2,
        );
      setScriptEnvironmentDraft({
        ...values,
        selectedEnvName: name || '',
        envName: name || '',
        mode: name ? 'edit' : 'create',
        isExpanded: true,
        nodeId: scriptNodeBinding?.nodeId,
        initial: {
          envName: name || '',
          builder: values.builder,
          config: values.config,
        },
      });
      setConfigError(null);
      if (name) startEnvironmentEdit();
      else startEnvironmentCreate();
    },
    [
      environments,
      builderDraft,
      defaultBuilder,
      scriptNodeBinding?.nodeId,
      setScriptEnvironmentDraft,
      startEnvironmentEdit,
      startEnvironmentCreate,
    ],
  );

  useEffect(() => {
    if (!draft && panelMode !== 'view')
      beginEditing(panelMode === 'edit' ? activeEnvironmentName : undefined);
  }, [draft, panelMode, activeEnvironmentName, beginEditing]);

  const getEnvUsageCount = useCallback(
    (name: string) =>
      nodeManager.nodes.filter(
        (node) => node.type === 'script' && node.data.op.environment === name,
      ).length,
    [nodeManager.nodes],
  );

  let parsedConfig: Record<string, unknown> = {};
  let jsonError: string | null = null;
  try {
    parsedConfig = objectConfig(JSON.parse(config));
  } catch (error) {
    jsonError = (error as Error).message;
  }
  const configKeys = Object.keys(parsedConfig).filter(
    (key) => typeof parsedConfig[key] === 'string',
  );
  const scriptText =
    typeof parsedConfig[activeCodeField] === 'string'
      ? (parsedConfig[activeCodeField] as string)
      : '';
  const handleScriptTextChange = (value: string) => {
    if (jsonError) return;
    parsedConfig[activeCodeField] = value;
    updateDraft({ config: JSON.stringify(parsedConfig, null, 2) });
  };

  const handleSave = () => {
    if (isSaveDisabled) return false;
    try {
      setDiagramProperties((prev) => ({
        ...prev,
        script_environments: {
          ...prev.script_environments,
          [envName]: {
            builder,
            config: JSON.parse(config),
          },
        },
      }));
      endEditing();
      selectEnvironment(envName);
      if (mode === 'create') {
        scriptNodeBinding?.assignEnvironment(envName);
      }
      showNotification(
        mode === 'create'
          ? `Environment '${envName}' created successfully`
          : `Environment '${envName}' saved successfully`,
        'success',
      );
      return true;
    } catch {
      setConfigError('Invalid JSON');
      return false;
    }
  };

  const handleCancel = () => {
    if (draft && isScriptEnvironmentDraftDirty(draft))
      setPendingLeave({ proceed: () => {}, discardOnly: true });
    else endEditing();
  };

  const handleDelete = () => {
    if (!activeEnvironmentName || getEnvUsageCount(activeEnvironmentName) > 0) {
      return;
    }

    setDiagramProperties((prev) => {
      const { [activeEnvironmentName]: _, ...rest } =
        prev.script_environments || {};
      return {
        ...prev,
        script_environments: rest,
        highlightedEnvironment:
          prev.highlightedEnvironment === activeEnvironmentName
            ? undefined
            : prev.highlightedEnvironment,
      };
    });
    showNotification(
      `Environment '${activeEnvironmentName}' deleted successfully`,
      'success',
    );
    selectEnvironment(undefined);
  };

  const isSaveDisabled =
    !!jsonError ||
    !!configError ||
    !!nameError ||
    !envName ||
    !builder ||
    !registeredBuilders.includes(builder);

  const selectedEnvironment = activeEnvironmentName
    ? environments[activeEnvironmentName]
    : undefined;
  const selectedMetadata = selectedEnvironment
    ? registry.scripting?.[selectedEnvironment.builder]
    : undefined;
  const boundNodeHasNoEnvironments =
    nodeControlsSelection && Object.keys(environments).length === 0;

  const renderEnvironmentActions = () => (
    <Stack direction="row" spacing={0.5} alignItems="center">
      {activeEnvironmentName && selectedEnvironment && (
        <>
          <Tooltip
            title={
              diagramProperties.highlightedEnvironment === activeEnvironmentName
                ? 'Hide nodes'
                : 'Show nodes'
            }
          >
            <IconButton
              aria-label={
                diagramProperties.highlightedEnvironment ===
                activeEnvironmentName
                  ? 'Hide nodes'
                  : 'Show nodes'
              }
              onClick={() => {
                setDiagramProperties((prev) => ({
                  ...prev,
                  highlightedEnvironment:
                    prev.highlightedEnvironment === activeEnvironmentName
                      ? undefined
                      : activeEnvironmentName,
                }));
              }}
            >
              <MaterialSymbol
                symbol={
                  diagramProperties.highlightedEnvironment ===
                  activeEnvironmentName
                    ? 'visibility_off'
                    : 'visibility'
                }
              />
            </IconButton>
          </Tooltip>
          <Tooltip title="Edit environment">
            <IconButton
              aria-label="Edit environment"
              onClick={() => {
                if (activeEnvironmentName) beginEditing(activeEnvironmentName);
              }}
            >
              <MaterialSymbol symbol="edit" />
            </IconButton>
          </Tooltip>
          <Tooltip
            title={
              getEnvUsageCount(activeEnvironmentName) > 0
                ? `Cannot delete: used by ${getEnvUsageCount(
                    activeEnvironmentName,
                  )} nodes`
                : 'Delete environment'
            }
          >
            <span>
              <IconButton
                aria-label="Delete environment"
                disabled={getEnvUsageCount(activeEnvironmentName) > 0}
                onClick={handleDelete}
              >
                <MaterialSymbol symbol="delete" />
              </IconButton>
            </span>
          </Tooltip>
        </>
      )}
    </Stack>
  );

  if (mode === 'view' && activeEnvironmentName && !selectedEnvironment) {
    return (
      <Stack spacing={2} sx={{ p: 2 }}>
        <Alert
          severity="warning"
          action={
            <Button
              onClick={() => requestEnvironmentLeave(() => beginEditing())}
            >
              Create environment
            </Button>
          }
        >
          Environment “{activeEnvironmentName}” is not defined.
        </Alert>
      </Stack>
    );
  }

  const windowAction = editorWindow
    ? 'Return environment to panel'
    : 'Open environment in separate window';
  const expandAction = expanded
    ? 'Collapse environment editor'
    : 'Expand environment editor';
  const editor = (
    <Stack
      spacing={2}
      sx={{
        p: 2,
        minWidth: 0,
        overflowY: 'auto',
        containerType: 'inline-size',
      }}
    >
      <Stack
        direction="row"
        spacing={0.5}
        alignItems="center"
        sx={{ flexWrap: 'nowrap' }}
      >
        {mode === 'view' && !nodeControlsSelection ? (
          <TextField
            select
            label="Select Environment"
            value={selectedEnvironmentName || ''}
            onChange={(event) => {
              if (event.target.value === CREATE_ENVIRONMENT_VALUE) {
                beginEditing();
                return;
              }
              selectEnvironment(event.target.value || undefined);
            }}
            fullWidth
          >
            {Object.keys(environments).length === 0 ? (
              <MenuItem disabled value="">
                No environments available
              </MenuItem>
            ) : (
              Object.keys(environments).map((name) => (
                <MenuItem key={name} value={name}>
                  {name}
                </MenuItem>
              ))
            )}
            <MenuItem value={CREATE_ENVIRONMENT_VALUE}>
              <Stack direction="row" spacing={1} alignItems="center">
                <MaterialSymbol symbol="add" />
                <span>Create new environment</span>
              </Stack>
            </MenuItem>
          </TextField>
        ) : mode !== 'view' ? (
          <Typography variant="h6" sx={{ flex: 1, whiteSpace: 'nowrap' }}>
            {mode === 'create' ? 'Create Environment' : 'Edit Environment'}
          </Typography>
        ) : null}
        {mode === 'view' ? (
          !boundNodeHasNoEnvironments &&
          activeEnvironmentName &&
          selectedEnvironment &&
          renderEnvironmentActions()
        ) : (
          <>
            <Tooltip title={windowAction}>
              <IconButton
                aria-label={windowAction}
                onClick={() => {
                  if (editorWindow) {
                    returnToPanel();
                    return;
                  }
                  const popup = window.open(
                    '',
                    '_blank',
                    'popup,width=1100,height=800',
                  );
                  if (popup) {
                    dockedLayout.current = { expanded, width };
                    setEditorWindow(popup);
                    setExpanded(false);
                  } else
                    showNotification(
                      'Allow pop-ups to open the environment editor.',
                      'warning',
                    );
                }}
              >
                <MaterialSymbol
                  symbol={editorWindow ? 'close_fullscreen' : 'open_in_new'}
                />
              </IconButton>
            </Tooltip>
            {!editorWindow && (
              <Tooltip title={expandAction}>
                <IconButton
                  aria-label={expandAction}
                  onClick={() => {
                    setExpanded(!expanded);
                  }}
                >
                  <MaterialSymbol
                    symbol={expanded ? 'fullscreen_exit' : 'fullscreen'}
                  />
                </IconButton>
              </Tooltip>
            )}
            <Button
              variant="contained"
              disabled={isSaveDisabled}
              onClick={handleSave}
            >
              Save
            </Button>
            <Button onClick={handleCancel}>Cancel</Button>
          </>
        )}
      </Stack>

      {mode === 'view' ? (
        activeEnvironmentName && selectedEnvironment ? (
          <ScriptEnvironmentPanel
            environmentName={activeEnvironmentName}
            environment={selectedEnvironment}
            metadata={selectedMetadata}
          />
        ) : boundNodeHasNoEnvironments ? (
          <Paper
            variant="outlined"
            sx={{ p: 3, textAlign: 'center', borderStyle: 'dashed' }}
          >
            <Stack spacing={2} alignItems="center">
              <Typography color="text.secondary">
                No environments are available for this script node.
              </Typography>
              <Button
                variant="contained"
                onClick={() => requestEnvironmentLeave(() => beginEditing())}
                disabled={registeredBuilders.length === 0}
              >
                Create environment
              </Button>
            </Stack>
          </Paper>
        ) : nodeControlsSelection ? (
          <Paper
            variant="outlined"
            sx={{ p: 3, textAlign: 'center', borderStyle: 'dashed' }}
          >
            <Typography color="text.secondary">
              Select an environment in the script node editor.
            </Typography>
          </Paper>
        ) : (
          <Paper
            variant="outlined"
            sx={{ p: 3, textAlign: 'center', borderStyle: 'dashed' }}
          >
            <Typography color="text.secondary">
              Select an environment to inspect its runtime and configuration.
            </Typography>
          </Paper>
        )
      ) : registeredBuilders.length === 0 ? (
        <Alert severity="warning">
          No script environment builders are available.
        </Alert>
      ) : (
        <Box
          sx={{
            display: 'grid',
            gridTemplateColumns: 'minmax(0, 1fr)',
            '@container (min-width: 700px)': {
              gridTemplateColumns:
                expanded || editorWindow
                  ? '260px minmax(0, 1fr)'
                  : 'minmax(0, 1fr)',
            },
            gap: 2,
            minWidth: 0,
          }}
        >
          {(expanded || editorWindow) && (
            <Paper variant="outlined" sx={{ minWidth: 0 }}>
              <Stack sx={{ p: 1 }}>
                <Button
                  startIcon={<MaterialSymbol symbol="add" />}
                  onClick={() => requestEnvironmentLeave(() => beginEditing())}
                >
                  New environment
                </Button>
              </Stack>
              <Divider />
              <List dense>
                {Object.keys(environments).map((name) => (
                  <ListItemButton
                    key={name}
                    selected={name === selectedEnvironmentName}
                    onClick={() => {
                      requestEnvironmentLeave(() => {
                        selectEnvironment(name);
                        beginEditing(name);
                      });
                    }}
                  >
                    <ListItemText primary={name} />
                  </ListItemButton>
                ))}
              </List>
            </Paper>
          )}

          <Stack spacing={2} minWidth={0}>
            <TextField
              label="Environment Name"
              value={envName}
              onChange={(event) => {
                updateDraft({ envName: event.target.value });
              }}
              disabled={mode === 'edit'}
              error={!!nameError}
              helperText={nameError}
              fullWidth
            />
            <Stack direction={{ xs: 'column', md: 'row' }} spacing={2}>
              <TextField
                select
                label="Builder"
                value={builder}
                onChange={(event) => {
                  updateDraft(builderDraft(event.target.value));
                  setConfigError(null);
                }}
                fullWidth
              >
                {registeredBuilders.map((registeredBuilder) => (
                  <MenuItem key={registeredBuilder} value={registeredBuilder}>
                    {registeredBuilder}
                  </MenuItem>
                ))}
              </TextField>
              {!plugin?.defaultCodeField && (
                <TextField
                  select
                  label="Code Field"
                  value={selectedCodeField}
                  onChange={(event) =>
                    updateDraft({ selectedCodeField: event.target.value })
                  }
                  fullWidth
                >
                  {configKeys.length === 0 ? (
                    <MenuItem disabled value="">
                      No string fields defined
                    </MenuItem>
                  ) : (
                    configKeys.map((key) => (
                      <MenuItem key={key} value={key}>
                        {key}
                      </MenuItem>
                    ))
                  )}
                </TextField>
              )}
              <TextField
                label="Scripting Language"
                value={registry.scripting?.[builder]?.language || 'python'}
                disabled
                fullWidth
              />
            </Stack>

            {plugin ? (
              <plugin.PropertiesForm
                config={config}
                onChange={(config) => updateDraft({ config })}
                onValidationError={setConfigError}
                mode={mode}
                registry={registry}
                scriptText={scriptText}
              />
            ) : (
              <TextField
                label="Builder Config (JSON)"
                multiline
                rows={4}
                value={config}
                onChange={(event) =>
                  updateDraft({ config: event.target.value })
                }
                error={!!jsonError}
                helperText={jsonError}
                fullWidth
                slotProps={{
                  htmlInput: { sx: { fontFamily: 'monospace' } },
                }}
              />
            )}

            {activeCodeField &&
            (plugin || configKeys.includes(activeCodeField)) ? (
              <>
                <Typography variant="caption" color="text.secondary">
                  Script ({activeCodeField})
                </Typography>
                <CodeMirror
                  ref={codeEditor}
                  onCreateEditor={(view) => {
                    if (!draft?.editor) return;
                    const { anchor, head, scrollTop, scrollLeft } =
                      draft.editor;
                    view.dispatch({
                      selection: {
                        anchor: Math.min(anchor, view.state.doc.length),
                        head: Math.min(head, view.state.doc.length),
                      },
                    });
                    view.scrollDOM.scrollTop = scrollTop;
                    view.scrollDOM.scrollLeft = scrollLeft;
                  }}
                  onUpdate={(update) => {
                    if (update.selectionSet) captureEditor(update.view);
                  }}
                  value={scriptText}
                  height={expanded || editorWindow ? '50vh' : '300px'}
                  extensions={[
                    ...(plugin?.codeExtensions?.() || []),
                    indentUnit.of('    '),
                    keymap.of([indentWithTab]),
                    EditorView.domEventHandlers({
                      scroll: (_event, view) => {
                        captureEditor(view);
                      },
                    }),
                  ]}
                  onChange={handleScriptTextChange}
                  theme="dark"
                />
              </>
            ) : (
              <Paper
                variant="outlined"
                sx={{
                  p: 3,
                  textAlign: 'center',
                  backgroundColor: 'action.hover',
                  borderStyle: 'dashed',
                }}
              >
                <Typography color="text.secondary">
                  Define and select a string config field to edit its script.
                </Typography>
              </Paper>
            )}
          </Stack>
        </Box>
      )}
    </Stack>
  );
  const leaveDialog = (
    <Dialog open={pendingLeave !== null} onClose={() => setPendingLeave(null)}>
      <DialogTitle>
        {pendingLeave?.discardOnly
          ? 'Discard environment changes?'
          : 'Save environment changes?'}
      </DialogTitle>
      {!pendingLeave?.discardOnly && (
        <DialogContent>
          This environment has changes. Save them before continuing?
        </DialogContent>
      )}
      <DialogActions>
        <Button onClick={() => setPendingLeave(null)}>Cancel</Button>
        <Button
          color="error"
          onClick={() => {
            const next = pendingLeave;
            setPendingLeave(null);
            endEditing();
            next?.proceed();
          }}
        >
          Discard
        </Button>
        {!pendingLeave?.discardOnly && (
          <Button
            variant="contained"
            disabled={isSaveDisabled}
            onClick={() => {
              if (handleSave()) {
                const next = pendingLeave;
                setPendingLeave(null);
                next?.proceed();
              }
            }}
          >
            Save &amp; Continue
          </Button>
        )}
      </DialogActions>
    </Dialog>
  );
  return (
    <>
      {editorWindow && (
        <Stack direction="row" sx={{ p: 2 }}>
          <Button onClick={() => editorWindow.focus()}>
            Focus environment window
          </Button>
          <IconButton
            aria-label="Return environment to panel"
            onClick={returnToPanel}
          >
            <MaterialSymbol symbol="close_fullscreen" />
          </IconButton>
        </Stack>
      )}
      <EnvironmentWindow popup={editorWindow}>
        {editor}
        {leaveDialog}
      </EnvironmentWindow>
    </>
  );
}
