import {
  createContext,
  type PropsWithChildren,
  type SetStateAction,
  useCallback,
  useContext,
  useMemo,
  useState,
} from 'react';
import {
  EMPTY_TRANSIENT_EDITOR_DRAFTS,
  isScriptEnvironmentDraftDirty,
  type ScriptEnvironmentEditorDraft,
  type TemplateRenameDraft,
  type TransientEditorDrafts,
} from './draft-workspace';

interface TransientEditorDraftContextValue {
  drafts: TransientEditorDrafts;
  replaceDrafts: (drafts: TransientEditorDrafts) => void;
  setScriptEnvironmentDraft: (
    draft: SetStateAction<ScriptEnvironmentEditorDraft | undefined>,
  ) => void;
  setOperationConfigDraft: (key: string, value: string | undefined) => void;
  clearOperationConfigDrafts: (keys: Iterable<string>) => void;
  setTemplateRenameDraft: (draft: TemplateRenameDraft | undefined) => void;
  clearDrafts: () => void;
  hasUncommittedBuffers: boolean;
}

const TransientEditorDraftContext =
  createContext<TransientEditorDraftContextValue | null>(null);

export function TransientEditorDraftProvider({ children }: PropsWithChildren) {
  const [drafts, setDrafts] = useState<TransientEditorDrafts>(
    EMPTY_TRANSIENT_EDITOR_DRAFTS,
  );

  const replaceDrafts = useCallback(
    (nextDrafts: TransientEditorDrafts) => setDrafts(nextDrafts),
    [],
  );
  const setScriptEnvironmentDraft = useCallback(
    (draft: SetStateAction<ScriptEnvironmentEditorDraft | undefined>) =>
      setDrafts((prev) => ({
        ...prev,
        scriptEnvironment:
          typeof draft === 'function' ? draft(prev.scriptEnvironment) : draft,
      })),
    [],
  );
  const setOperationConfigDraft = useCallback(
    (key: string, value: string | undefined) =>
      setDrafts((prev) => {
        const operationConfigs = { ...prev.operationConfigs };
        if (value === undefined) {
          delete operationConfigs[key];
        } else {
          operationConfigs[key] = value;
        }
        return { ...prev, operationConfigs };
      }),
    [],
  );
  const clearOperationConfigDrafts = useCallback(
    (keys: Iterable<string>) =>
      setDrafts((prev) => {
        const operationConfigs = { ...prev.operationConfigs };
        let changed = false;
        for (const key of keys) {
          const nodeKey = `node:${key}:config`;
          const scriptKey = `script:${key}:config`;
          if (nodeKey in operationConfigs) {
            delete operationConfigs[nodeKey];
            changed = true;
          }
          if (scriptKey in operationConfigs) {
            delete operationConfigs[scriptKey];
            changed = true;
          }
        }
        return changed ? { ...prev, operationConfigs } : prev;
      }),
    [],
  );
  const setTemplateRenameDraft = useCallback(
    (draft: TemplateRenameDraft | undefined) =>
      setDrafts((prev) => ({ ...prev, templateRename: draft })),
    [],
  );
  const clearDrafts = useCallback(
    () => setDrafts(EMPTY_TRANSIENT_EDITOR_DRAFTS),
    [],
  );

  const value = useMemo<TransientEditorDraftContextValue>(
    () => ({
      drafts,
      replaceDrafts,
      setScriptEnvironmentDraft,
      setOperationConfigDraft,
      clearOperationConfigDrafts,
      setTemplateRenameDraft,
      clearDrafts,
      hasUncommittedBuffers:
        (drafts.scriptEnvironment !== undefined &&
          isScriptEnvironmentDraftDirty(drafts.scriptEnvironment)) ||
        drafts.templateRename !== undefined ||
        Object.keys(drafts.operationConfigs).length > 0,
    }),
    [
      clearDrafts,
      clearOperationConfigDrafts,
      drafts,
      replaceDrafts,
      setOperationConfigDraft,
      setScriptEnvironmentDraft,
      setTemplateRenameDraft,
    ],
  );

  return (
    <TransientEditorDraftContext.Provider value={value}>
      {children}
    </TransientEditorDraftContext.Provider>
  );
}

export function useTransientEditorDrafts(): TransientEditorDraftContextValue {
  const value = useContext(TransientEditorDraftContext);
  if (!value) {
    throw new Error(
      'useTransientEditorDrafts must be used within TransientEditorDraftProvider',
    );
  }
  return value;
}
