import type { Viewport } from '@xyflow/react';
import equal from 'fast-deep-equal';
import type { DiagramProperties } from './diagram-properties-provider';
import type { DiagramSidePanelState } from './diagram-side-panel-controller';
import type { PersistedState } from './persist-state';
import type { Diagram, SectionTemplate } from './types/api';

const DRAFT_KEY_PREFIX = 'crossflow-diagram-editor:draft:v1:';

export interface ScriptEnvironmentEditorDraft {
  selectedEnvName: string;
  nodeId?: string;
  mode: 'edit' | 'create';
  envName: string;
  builder: string;
  config: string;
  selectedCodeField: string;
  isExpanded: boolean;
  width?: number;
  editor?: {
    anchor: number;
    head: number;
    scrollTop: number;
    scrollLeft: number;
  };
  initial?: { envName: string; builder: string; config: string };
}

export function isScriptEnvironmentDraftDirty(
  draft: ScriptEnvironmentEditorDraft,
): boolean {
  if (!draft.initial) return true;
  if (
    draft.envName !== draft.initial.envName ||
    draft.builder !== draft.initial.builder
  )
    return true;
  try {
    return !equal(JSON.parse(draft.config), JSON.parse(draft.initial.config));
  } catch {
    return draft.config !== draft.initial.config;
  }
}

export interface TemplateRenameDraft {
  target: string;
  newId: string;
}

export interface TransientEditorDrafts {
  scriptEnvironment?: ScriptEnvironmentEditorDraft;
  operationConfigs: Record<string, string>;
  templateRename?: TemplateRenameDraft;
}

export const EMPTY_TRANSIENT_EDITOR_DRAFTS: TransientEditorDrafts = {
  operationConfigs: {},
};

export interface ActiveTemplateDraft {
  templateId: string;
  graph: PersistedState;
}

export interface DraftWorkspaceContent {
  mainGraph: PersistedState;
  activeTemplate?: ActiveTemplateDraft;
  templates: Record<string, SectionTemplate>;
  diagramProperties: Omit<DiagramProperties, 'highlightedEnvironment'>;
  sourceExtensions?: Diagram['extensions'];
  filename: string | null;
  transientEditors: TransientEditorDrafts;
  presentation?: { sidePanel: DiagramSidePanelState; viewport: Viewport };
}

export interface DraftWorkspaceV1 extends DraftWorkspaceContent {
  version: 1;
  savedAt: number;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isPersistedGraph(value: unknown): value is PersistedState {
  return (
    isRecord(value) && Array.isArray(value.nodes) && Array.isArray(value.edges)
  );
}

function isStringRecord(value: unknown): value is Record<string, string> {
  return isRecord(value) && Object.values(value).every(isString);
}

function isString(value: unknown): value is string {
  return typeof value === 'string';
}

function isScriptEnvironmentEditorDraft(
  value: unknown,
): value is ScriptEnvironmentEditorDraft {
  return (
    isRecord(value) &&
    isString(value.selectedEnvName) &&
    (value.mode === 'edit' || value.mode === 'create') &&
    isString(value.envName) &&
    isString(value.builder) &&
    isString(value.config) &&
    isString(value.selectedCodeField) &&
    typeof value.isExpanded === 'boolean'
  );
}

function isTemplateRenameDraft(value: unknown): value is TemplateRenameDraft {
  return isRecord(value) && isString(value.target) && isString(value.newId);
}

function isActiveTemplateDraft(value: unknown): value is ActiveTemplateDraft {
  return (
    isRecord(value) &&
    isString(value.templateId) &&
    isPersistedGraph(value.graph)
  );
}

function isTransientEditorDrafts(
  value: unknown,
): value is TransientEditorDrafts {
  return (
    isRecord(value) &&
    isStringRecord(value.operationConfigs) &&
    (value.scriptEnvironment === undefined ||
      isScriptEnvironmentEditorDraft(value.scriptEnvironment)) &&
    (value.templateRename === undefined ||
      isTemplateRenameDraft(value.templateRename))
  );
}

export function isDraftWorkspaceV1(value: unknown): value is DraftWorkspaceV1 {
  if (!isRecord(value) || value.version !== 1) {
    return false;
  }
  if (typeof value.savedAt !== 'number' || !isPersistedGraph(value.mainGraph)) {
    return false;
  }
  if (
    !isRecord(value.templates) ||
    !isRecord(value.diagramProperties) ||
    !isTransientEditorDrafts(value.transientEditors)
  ) {
    return false;
  }
  if (value.filename !== null && typeof value.filename !== 'string') {
    return false;
  }
  if (
    (value.activeTemplate !== undefined &&
      !isActiveTemplateDraft(value.activeTemplate)) ||
    (value.sourceExtensions !== undefined && !isRecord(value.sourceExtensions))
  ) {
    return false;
  }
  return true;
}

export function draftStorageKey(pathname: string): string {
  return `${DRAFT_KEY_PREFIX}${pathname}`;
}

function currentPathname(): string {
  return window.location.pathname;
}

function currentStorage(): Storage {
  return window.sessionStorage;
}

export function readDraftWorkspace(
  storage: Storage = currentStorage(),
  pathname: string = currentPathname(),
): DraftWorkspaceV1 | null {
  const serialized = storage.getItem(draftStorageKey(pathname));
  if (serialized === null) {
    return null;
  }
  const parsed: unknown = JSON.parse(serialized);
  if (!isDraftWorkspaceV1(parsed)) {
    throw new Error('unsupported or malformed diagram editor draft');
  }
  return parsed;
}

export function writeDraftWorkspace(
  content: DraftWorkspaceContent,
  storage: Storage = currentStorage(),
  pathname: string = currentPathname(),
): DraftWorkspaceV1 {
  const draft: DraftWorkspaceV1 = {
    ...content,
    version: 1,
    savedAt: Date.now(),
  };
  storage.setItem(draftStorageKey(pathname), JSON.stringify(draft));
  return draft;
}

export function clearDraftWorkspace(
  storage: Storage = currentStorage(),
  pathname: string = currentPathname(),
): void {
  storage.removeItem(draftStorageKey(pathname));
}

export function draftWorkspaceFingerprint(
  content: DraftWorkspaceContent,
): string {
  const { presentation: _presentation, transientEditors, ...diagram } = content;
  const draft = transientEditors.scriptEnvironment;
  return JSON.stringify({
    ...diagram,
    transientEditors: {
      ...transientEditors,
      scriptEnvironment:
        draft && isScriptEnvironmentDraftDirty(draft)
          ? {
              mode: draft.mode,
              envName: draft.envName,
              builder: draft.builder,
              config: draft.config,
            }
          : undefined,
    },
  });
}
