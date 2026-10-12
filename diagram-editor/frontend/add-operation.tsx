import {
  Button,
  ButtonGroup,
  Stack,
  styled,
  TextField,
  Typography,
} from '@mui/material';
import type { NodeAddChange, XYPosition } from '@xyflow/react';
import React from 'react';
import { EditorMode, useEditorMode } from './editor-mode';
import { useNodeManager } from './node-manager';
import {
  type DiagramEditorNode,
  getAddOperationIcon,
  isOperationNode,
} from './nodes';
import { getVisibleAddOperations } from './utils/add-operation-catalog';
import { joinNamespaces, ROOT_NAMESPACE } from './utils/namespace';

const StyledOperationButton = styled(Button)({
  justifyContent: 'flex-start',
});

export interface AddOperationSelection {
  primaryNodeId: string;
  changes: NodeAddChange<DiagramEditorNode>[];
}

export interface AddOperationProps {
  parentId?: string;
  newNodePosition: XYPosition;
  onAdd?: (selection: AddOperationSelection) => void;
}

function AddOperation({ parentId, newNodePosition, onAdd }: AddOperationProps) {
  const [editorMode] = useEditorMode();
  const nodeManager = useNodeManager();
  const [search, setSearch] = React.useState('');
  const namespace = React.useMemo(() => {
    const parentNode = parentId && nodeManager.tryGetNode(parentId);
    if (!parentNode || !isOperationNode(parentNode)) {
      return ROOT_NAMESPACE;
    }
    return joinNamespaces(parentNode.data.namespace, parentNode.data.opId);
  }, [parentId, nodeManager]);
  const compatibleOperations = React.useMemo(() => {
    return getVisibleAddOperations({
      isTemplateMode: editorMode.mode === EditorMode.Template,
      namespace,
    });
  }, [editorMode.mode, namespace]);

  const operations = React.useMemo(() => {
    const trimmedSearch = search.trim().toLowerCase();
    if (!trimmedSearch) {
      return compatibleOperations;
    }

    return compatibleOperations.filter((operation) =>
      operation.label.toLowerCase().includes(trimmedSearch),
    );
  }, [compatibleOperations, search]);

  const emptyMessage = React.useMemo(() => {
    if (operations.length > 0) {
      return null;
    }

    return search.trim()
      ? 'No operations match this filter.'
      : 'No operations are available here yet.';
  }, [operations.length, search]);

  return (
    <Stack spacing={1} sx={{ px: 1.5, pt: 1.5, pb: 1.5, width: 260 }}>
      <Typography variant="subtitle2">Add operation</Typography>
      <TextField
        size="small"
        placeholder="Filter operations"
        value={search}
        onChange={(event) => setSearch(event.target.value)}
      />
      {operations.length > 0 && (
        <ButtonGroup
          orientation="vertical"
          variant="contained"
          size="small"
          aria-label="Add operation button group"
          sx={{ width: '100%' }}
        >
          {operations.map((operation) => (
            <StyledOperationButton
              key={operation.key}
              startIcon={getAddOperationIcon(operation.key)}
              onClick={() => {
                const changes = operation.createChanges({
                  namespace,
                  parentId,
                  newNodePosition,
                  nodeManager,
                });
                const primaryNodeId = changes[0]?.item.id;
                if (!primaryNodeId) {
                  return;
                }
                onAdd?.({ changes, primaryNodeId });
              }}
            >
              {operation.label}
            </StyledOperationButton>
          ))}
        </ButtonGroup>
      )}
      {emptyMessage && <Typography variant="body2">{emptyMessage}</Typography>}
    </Stack>
  );
}

export default AddOperation;
