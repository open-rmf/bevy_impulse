import {
  clearDraftWorkspace,
  type DraftWorkspaceContent,
  draftStorageKey,
  draftWorkspaceFingerprint,
  readDraftWorkspace,
  writeDraftWorkspace,
} from './draft-workspace';

const content: DraftWorkspaceContent = {
  mainGraph: { nodes: [], edges: [] },
  templates: {},
  diagramProperties: { description: 'work in progress' },
  filename: 'draft.json',
  transientEditors: {
    operationConfigs: { 'node:one:config': '{ invalid' },
    scriptEnvironment: {
      selectedEnvName: '',
      mode: 'create',
      envName: 'python',
      builder: 'process-bound-python',
      config: '{"script":"print(1)"}',
      selectedCodeField: 'script',
      isExpanded: false,
    },
  },
};

beforeEach(() => {
  sessionStorage.clear();
});

test('uses a pathname-scoped session storage key', () => {
  expect(draftStorageKey('/editor/a')).not.toBe(draftStorageKey('/editor/b'));
});

test('round trips a versioned workspace draft', () => {
  const saved = writeDraftWorkspace(content, sessionStorage, '/editor');
  expect(saved.version).toBe(1);
  expect(readDraftWorkspace(sessionStorage, '/editor')).toEqual(saved);
});

test('rejects malformed and unsupported drafts', () => {
  sessionStorage.setItem(draftStorageKey('/editor'), '{"version":2}');
  expect(() => readDraftWorkspace(sessionStorage, '/editor')).toThrow(
    'unsupported or malformed',
  );
});

test.each([
  [
    'an operation configuration with a non-string value',
    (draft: Record<string, unknown>) => {
      (draft.transientEditors as Record<string, unknown>).operationConfigs = {
        'node:one:config': 1,
      };
    },
  ],
  [
    'a script environment with an unsupported mode',
    (draft: Record<string, unknown>) => {
      (
        (draft.transientEditors as Record<string, unknown>)
          .scriptEnvironment as Record<string, unknown>
      ).mode = 'delete';
    },
  ],
  [
    'a script environment with a non-string field',
    (draft: Record<string, unknown>) => {
      (
        (draft.transientEditors as Record<string, unknown>)
          .scriptEnvironment as Record<string, unknown>
      ).envName = 1;
    },
  ],
  [
    'a malformed template rename draft',
    (draft: Record<string, unknown>) => {
      (draft.transientEditors as Record<string, unknown>).templateRename = {
        target: 'old',
      };
    },
  ],
  [
    'non-record source extensions',
    (draft: Record<string, unknown>) => {
      draft.sourceExtensions = [];
    },
  ],
])('rejects a separately serialized draft containing %s', (_, mutate) => {
  const draft = JSON.parse(
    JSON.stringify(writeDraftWorkspace(content, sessionStorage, '/editor')),
  ) as Record<string, unknown>;
  mutate(draft);
  sessionStorage.setItem(draftStorageKey('/editor'), JSON.stringify(draft));

  expect(() => readDraftWorkspace(sessionStorage, '/editor')).toThrow(
    'unsupported or malformed',
  );
});

test('clears only the selected editor pathname', () => {
  writeDraftWorkspace(content, sessionStorage, '/editor/a');
  writeDraftWorkspace(content, sessionStorage, '/editor/b');
  clearDraftWorkspace(sessionStorage, '/editor/a');
  expect(readDraftWorkspace(sessionStorage, '/editor/a')).toBeNull();
  expect(readDraftWorkspace(sessionStorage, '/editor/b')).not.toBeNull();
});

test('fingerprint excludes the generated save timestamp', () => {
  expect(draftWorkspaceFingerprint(content)).toBe(
    draftWorkspaceFingerprint(JSON.parse(JSON.stringify(content))),
  );
});

test('surfaces storage write failures', () => {
  const setItem = jest
    .spyOn(Storage.prototype, 'setItem')
    .mockImplementation(() => {
      throw new DOMException('quota exceeded', 'QuotaExceededError');
    });
  expect(() => writeDraftWorkspace(content, sessionStorage, '/editor')).toThrow(
    'quota exceeded',
  );
  setItem.mockRestore();
});

test('presentation and untouched environment forms do not dirty the workflow', () => {
  const clean = { ...content, transientEditors: { operationConfigs: {} } };
  const draft = content.transientEditors.scriptEnvironment!;
  const opened = {
    ...clean,
    transientEditors: {
      ...clean.transientEditors,
      scriptEnvironment: {
        ...draft,
        initial: {
          envName: draft.envName,
          builder: draft.builder,
          config: draft.config,
        },
      },
    },
    presentation: {
      sidePanel: {
        open: true,
        tab: 'environments',
        mode: 'create',
        expanded: false,
        width: 610,
      },
      viewport: { x: 12, y: 34, zoom: 1.5 },
    },
  };
  expect(draftWorkspaceFingerprint(opened as DraftWorkspaceContent)).toBe(
    draftWorkspaceFingerprint(clean),
  );
  opened.transientEditors.scriptEnvironment.config = '{"script":"changed"}';
  expect(draftWorkspaceFingerprint(opened as DraftWorkspaceContent)).not.toBe(
    draftWorkspaceFingerprint(clean),
  );
});
