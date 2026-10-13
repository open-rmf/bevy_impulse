import {
  Autocomplete,
  IconButton,
  MenuItem,
  Stack,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material';
import { useMemo, useState } from 'react';
import { useDiagramProperties } from '../diagram-properties-provider';
import { useDiagramSidePanel } from '../diagram-side-panel-controller';
import { MaterialSymbol } from '../nodes';
import { useTransientEditorDrafts } from '../transient-editor-drafts';
import BaseEditOperationForm, {
  type BaseEditOperationFormProps,
} from './base-edit-operation-form';
import { useScriptEnvironmentNavigation } from './use-script-environment-navigation';

export type ScriptFormProps = BaseEditOperationFormProps<'script'>;

const CREATE_ENVIRONMENT_VALUE = '__create_script_environment__';

function ScriptForm(props: ScriptFormProps) {
  const [diagramProperties] = useDiagramProperties();
  const { drafts, setOperationConfigDraft } = useTransientEditorDrafts();
  const environments = diagramProperties.script_environments || {};
  const openScriptEnvironment = useScriptEnvironmentNavigation();
  const {
    state: { open: panelOpen, tab: panelTab },
    close: closePanel,
    startEnvironmentCreate,
    requestEnvironmentLeave,
  } = useDiagramSidePanel();

  // Track raw string of node config to support JSON editing
  const configDraftKey = `script:${props.node.id}:config`;
  const [configValue, setConfigValue] = useState(
    () =>
      drafts.operationConfigs[configDraftKey] ??
      (props.node.data.op.config
        ? JSON.stringify(props.node.data.op.config, null, 2)
        : ''),
  );

  const configError = useMemo(() => {
    if (configValue === '') return false;
    try {
      JSON.parse(configValue);
      return false;
    } catch {
      return true;
    }
  }, [configValue]);

  const handleEnvChange = (envName: string) =>
    requestEnvironmentLeave(() => {
      const updatedNode = {
        ...props.node,
        data: {
          ...props.node.data,
          op: {
            ...props.node.data.op,
            environment: envName,
          },
        },
      };
      props.onChange?.({
        type: 'replace',
        id: props.node.id,
        item: updatedNode,
      });
      openScriptEnvironment(envName, false);
    });

  // Real-time parse python entrypoint functions from selected environment
  const selectedEnvName = props.node.data.op.environment;
  const selectedEnv = selectedEnvName
    ? environments[selectedEnvName]
    : undefined;

  const scriptText = useMemo(() => {
    if (!selectedEnv) return '';
    if (selectedEnv.builder === 'process-bound-python') {
      const pbConfig = selectedEnv.config as
        | Record<string, unknown>
        | undefined;
      return (pbConfig?.script as string) || '';
    }
    const envRecord = selectedEnv as unknown as Record<string, unknown>;
    return (envRecord.script as string) || '';
  }, [selectedEnv]);

  const availableFunctions = useMemo(() => {
    if (!scriptText) return [];
    const regex = /^[ \t]*(?:async\s+)?def\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(/gm;
    const functions: string[] = [];
    regex.lastIndex = 0;
    while (true) {
      const match = regex.exec(scriptText);
      if (match === null) {
        break;
      }
      functions.push(match[1]);
    }
    return functions;
  }, [scriptText]);

  return (
    <BaseEditOperationForm {...props}>
      <TextField
        label="Display Text"
        value={props.node.data.op.display_text || ''}
        onChange={(ev) => {
          const updatedNode = { ...props.node };
          updatedNode.data.op.display_text = ev.target.value || undefined;
          props.onChange?.({
            type: 'replace',
            id: props.node.id,
            item: updatedNode,
          });
        }}
      />

      <Stack direction="row" spacing={1} alignItems="center" minWidth={0}>
        <TextField
          select
          label="Script Environment"
          value={props.node.data.op.environment || ''}
          onChange={(e) => {
            if (e.target.value === CREATE_ENVIRONMENT_VALUE) {
              requestEnvironmentLeave(startEnvironmentCreate);
              return;
            }
            handleEnvChange(e.target.value);
          }}
          fullWidth
        >
          {selectedEnvName && !selectedEnv && (
            <MenuItem disabled value={selectedEnvName}>
              {selectedEnvName} (missing)
            </MenuItem>
          )}
          {Object.keys(environments).length === 0 && !selectedEnvName ? (
            <MenuItem disabled value="">
              <Typography variant="caption" color="text.disabled">
                No environments available, please create one
              </Typography>
            </MenuItem>
          ) : (
            Object.keys(environments).map((envName) => (
              <MenuItem key={envName} value={envName}>
                {envName}
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

        <Tooltip title="Toggle script environment panel">
          <IconButton
            aria-label="Toggle script environment panel"
            onClick={() => {
              if (panelOpen && panelTab === 'environments') {
                closePanel();
                return;
              }
              openScriptEnvironment(
                props.node.data.op.environment || undefined,
                false,
              );
            }}
          >
            <MaterialSymbol symbol="settings" />
          </IconButton>
        </Tooltip>
      </Stack>

      {/* Entrypoint dropdown populated via real-time script parser */}
      <Autocomplete
        freeSolo
        autoSelect
        options={availableFunctions}
        value={props.node.data.op.run || ''}
        onChange={(_, val) => {
          const updatedNode = { ...props.node };
          updatedNode.data.op.run = val ?? '';
          props.onChange?.({
            type: 'replace',
            id: props.node.id,
            item: updatedNode,
          });
        }}
        onInputChange={(_, val) => {
          const updatedNode = { ...props.node };
          updatedNode.data.op.run = val;
          props.onChange?.({
            type: 'replace',
            id: props.node.id,
            item: updatedNode,
          });
        }}
        renderInput={(params) => (
          <TextField
            {...params}
            required
            label="Entrypoint Function (run)"
            placeholder="Select or type the script function"
          />
        )}
      />

      <TextField
        multiline
        rows={4}
        label="Node Config (JSON)"
        value={configValue}
        onChange={(ev) => {
          setConfigValue(ev.target.value);
          try {
            const updatedNode = { ...props.node };
            updatedNode.data.op.config =
              ev.target.value === '' ? undefined : JSON.parse(ev.target.value);
            props.onChange?.({
              type: 'replace',
              id: props.node.id,
              item: updatedNode,
            });
            setOperationConfigDraft(configDraftKey, undefined);
          } catch {
            setOperationConfigDraft(configDraftKey, ev.target.value);
          }
        }}
        error={configError}
        slotProps={{
          htmlInput: {
            sx: { fontFamily: 'monospace', whiteSpace: 'nowrap' },
          },
        }}
      />
    </BaseEditOperationForm>
  );
}

export default ScriptForm;
