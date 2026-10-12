import { fireEvent, render, screen } from '@testing-library/react';
import AddOperation from './add-operation';

const mockCreateTransformChanges = jest.fn(() => [
  {
    type: 'add' as const,
    item: { id: 'new-transform-node' },
  },
]);

const mockOperations = [
  {
    key: 'transform',
    label: 'Transform',
    createChanges: mockCreateTransformChanges,
  },
  {
    key: 'fork_clone',
    label: 'Fork Clone',
    createChanges: () => [
      {
        type: 'add' as const,
        item: { id: 'new-fork-clone-node' },
      },
    ],
  },
];

let visibleOperations = mockOperations;
const mockEditorMode = [{ mode: 0 }];
const mockNodeManager = {
  tryGetNode: () => null,
};

jest.mock('./editor-mode', () => ({
  EditorMode: { Normal: 0, Template: 1 },
  useEditorMode: () => mockEditorMode,
}));

jest.mock('./node-manager', () => ({
  useNodeManager: () => mockNodeManager,
}));

jest.mock('./utils/add-operation-catalog', () => ({
  getVisibleAddOperations: () => visibleOperations,
}));

describe('AddOperation', () => {
  beforeEach(() => {
    visibleOperations = mockOperations;
    jest.clearAllMocks();
  });

  test('renders operation suggestions with corresponding icons', () => {
    render(<AddOperation newNodePosition={{ x: 10, y: 20 }} />);

    const transformButton = screen.getByRole('button', { name: /Transform/ });
    const forkCloneButton = screen.getByRole('button', { name: /Fork Clone/ });

    expect(transformButton).toBeInTheDocument();
    expect(forkCloneButton).toBeInTheDocument();

    const transformIcon = transformButton.querySelector(
      '.material-symbols-outlined',
    );
    const forkCloneIcon = forkCloneButton.querySelector(
      '.material-symbols-outlined',
    );

    expect(transformIcon).toBeInTheDocument();
    expect(forkCloneIcon).toBeInTheDocument();
    expect(transformIcon?.textContent).toBe('change_circle');
    expect(forkCloneIcon?.textContent).toBe('content_copy');
  });

  test('calls onAdd with changes and primaryNodeId when an operation is clicked', () => {
    const onAdd = jest.fn();

    render(
      <AddOperation
        parentId="parent-1"
        newNodePosition={{ x: 10, y: 20 }}
        onAdd={onAdd}
      />,
    );

    fireEvent.click(screen.getByRole('button', { name: /Transform/ }));

    expect(mockCreateTransformChanges).toHaveBeenCalledWith({
      namespace: '',
      parentId: 'parent-1',
      newNodePosition: { x: 10, y: 20 },
      nodeManager: mockNodeManager,
    });
    expect(onAdd).toHaveBeenCalledWith({
      primaryNodeId: 'new-transform-node',
      changes: [
        {
          type: 'add',
          item: { id: 'new-transform-node' },
        },
      ],
    });
  });

  test('does not throw when clicked and onAdd is omitted', () => {
    render(<AddOperation newNodePosition={{ x: 10, y: 20 }} />);
    expect(() => {
      fireEvent.click(screen.getByRole('button', { name: /Transform/ }));
    }).not.toThrow();
  });

  test('filters operations by search text and shows no match message', () => {
    render(<AddOperation newNodePosition={{ x: 10, y: 20 }} />);

    const searchInput = screen.getByPlaceholderText('Filter operations');
    fireEvent.change(searchInput, { target: { value: 'fork' } });

    expect(
      screen.queryByRole('button', { name: /Transform/ }),
    ).not.toBeInTheDocument();
    expect(
      screen.getByRole('button', { name: /Fork Clone/ }),
    ).toBeInTheDocument();

    fireEvent.change(searchInput, { target: { value: 'non-existent' } });
    expect(screen.queryByRole('button')).not.toBeInTheDocument();
    expect(
      screen.getByText('No operations match this filter.'),
    ).toBeInTheDocument();
  });

  test('shows empty state message when no operations are available', () => {
    visibleOperations = [];
    render(<AddOperation newNodePosition={{ x: 10, y: 20 }} />);

    expect(screen.queryByRole('button')).not.toBeInTheDocument();
    expect(
      screen.getByText('No operations are available here yet.'),
    ).toBeInTheDocument();
  });
});
