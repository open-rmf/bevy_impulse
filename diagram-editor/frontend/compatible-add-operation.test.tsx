import { fireEvent, render, screen } from '@testing-library/react';
import { CompatibleAddOperation } from './compatible-add-operation';

const mockCreateChanges = jest.fn(() => [
  { type: 'add', item: { id: 'candidate-node' } },
]);
const mockCandidate = {
  key: 'candidate',
  label: 'Candidate operation',
  createChanges: mockCreateChanges,
};
const mockCandidates = [
  mockCandidate,
  { key: 'transform', label: 'Transform', createChanges: mockCreateChanges },
  { key: 'fork_clone', label: 'Fork Clone', createChanges: mockCreateChanges },
  {
    key: 'node:calculator',
    label: 'Calculator',
    createChanges: mockCreateChanges,
  },
];
const mockSetMenuPreview = jest.fn();
const mockNodeManager = { tryGetNode: () => ({ id: 'source-node' }) };
const mockRegistry = {};
const mockMode = [{ mode: 0 }];
jest.mock('./connection-compatibility-provider', () => ({
  useCompatibilityChecker: () => ({ setMenuPreview: mockSetMenuPreview }),
}));
jest.mock('./editor-mode', () => ({
  EditorMode: { Normal: 0, Template: 1 },
  useEditorMode: () => mockMode,
}));
jest.mock('./node-manager', () => ({ useNodeManager: () => mockNodeManager }));
jest.mock('./registry-provider', () => ({ useRegistry: () => mockRegistry }));
jest.mock('./utils/add-operation-catalog', () => ({
  filterCompatibleAddOperations: (candidates: unknown[]) => candidates,
  getAddOperationCandidates: () => mockCandidates,
  getVisibleAddOperations: () => [],
}));

beforeEach(() => {
  mockCreateChanges.mockClear();
  mockSetMenuPreview.mockClear();
});

function menu(
  sourceHandleType: 'source' | 'target' = 'source',
  onAdd = jest.fn(),
) {
  return render(
    <CompatibleAddOperation
      newNodePosition={{ x: 0, y: 0 }}
      sourceConnection={{
        sourceNodeId: 'source-node',
        sourceHandle: null,
        sourceHandleType,
      }}
      onAdd={onAdd}
    />,
  );
}

test('offers structural choices without constructing hypothetical nodes until inspected', () => {
  const onAdd = jest.fn();
  menu('source', onAdd);
  const button = screen.getByRole('button', { name: /Candidate operation/ });
  expect(mockCreateChanges).not.toHaveBeenCalled();
  fireEvent.mouseEnter(button);
  expect(mockSetMenuPreview).toHaveBeenLastCalledWith({
    connection: {
      source: 'source-node',
      sourceHandle: null,
      target: 'candidate-node',
      targetHandle: null,
    },
    nodeChanges: [{ type: 'add', item: { id: 'candidate-node' } }],
  });
  fireEvent.click(button);
  expect(onAdd).toHaveBeenCalledWith({
    changes: [{ type: 'add', item: { id: 'candidate-node' } }],
    primaryNodeId: 'candidate-node',
  });
  fireEvent.mouseLeave(button);
  expect(mockSetMenuPreview).toHaveBeenLastCalledWith(null);
});

test('keyboard focus previews previous operations and closing clears the preview', () => {
  const { unmount } = menu('target');
  expect(screen.getByText('Add previous operation')).toBeInTheDocument();
  fireEvent.focus(screen.getByRole('button', { name: /Candidate operation/ }));
  expect(mockSetMenuPreview).toHaveBeenLastCalledWith(
    expect.objectContaining({
      connection: {
        source: 'candidate-node',
        sourceHandle: null,
        target: 'source-node',
        targetHandle: null,
      },
    }),
  );
  unmount();
  expect(mockSetMenuPreview).toHaveBeenLastCalledWith(null);
});

test('renders specific icons for compatible operations and registry builders', () => {
  menu();
  for (const [label, symbol] of [
    ['Transform', 'change_circle'],
    ['Fork Clone', 'content_copy'],
    ['Calculator', 'line_start_circle'],
  ]) {
    const button = screen.getByRole('button', { name: new RegExp(label) });
    expect(
      button.querySelector('.material-symbols-outlined'),
    ).toHaveTextContent(symbol);
  }
});
