import {
  Button,
  ButtonGroup,
  Stack,
  styled,
  TextField,
  Typography,
} from '@mui/material';
import type { XYPosition } from '@xyflow/react';
import React from 'react';
import type { AddOperationSelection } from './add-operation';
import { useCompatibilityChecker } from './connection-compatibility-provider';
import { EditorMode, useEditorMode } from './editor-mode';
import { useNodeManager } from './node-manager';
import { getAddOperationIcon, isOperationNode } from './nodes';
import { useRegistry } from './registry-provider';
import {
  filterCompatibleAddOperations,
  getAddOperationCandidates,
  getVisibleAddOperations,
} from './utils/add-operation-catalog';
import { createConnectionFromHandles } from './utils/connection';
import { joinNamespaces, ROOT_NAMESPACE } from './utils/namespace';

const StyledOperationButton = styled(Button)({
  justifyContent: 'flex-start',
});

export interface CompatibleAddOperationProps {
  parentId?: string;
  newNodePosition: XYPosition;
  sourceConnection: {
    sourceNodeId: string;
    sourceHandle: string | null;
    sourceHandleType: 'source' | 'target';
  };
  onAdd?: (selection: AddOperationSelection) => void;
  onContentChange?: () => void;
}

export function CompatibleAddOperation({
  parentId,
  newNodePosition,
  sourceConnection,
  onAdd,
  onContentChange,
}: CompatibleAddOperationProps) {
  const registry = useRegistry();
  const nodeManager = useNodeManager();
  const { setMenuPreview } = useCompatibilityChecker();
  const [editorMode] = useEditorMode();
  const [search, setSearch] = React.useState('');
  const [inspectedKey, setInspectedKey] = React.useState<string | null>(null);

  const namespace = React.useMemo(() => {
    const parentNode = parentId && nodeManager.tryGetNode(parentId);
    if (!parentNode || !isOperationNode(parentNode)) {
      return ROOT_NAMESPACE;
    }
    return joinNamespaces(parentNode.data.namespace, parentNode.data.opId);
  }, [parentId, nodeManager]);

  const candidates = React.useMemo(() => {
    const sourceNode = nodeManager.tryGetNode(sourceConnection.sourceNodeId);
    if (!sourceNode) {
      return [];
    }

    const visibleBuiltins = getVisibleAddOperations({
      isTemplateMode: editorMode.mode === EditorMode.Template,
      namespace,
    });
    const allCandidates = getAddOperationCandidates(registry, {
      includeGenericNode: false,
      includeRegistryNodes: true,
      includeBuiltins: true,
      builtins: visibleBuiltins,
    });

    return filterCompatibleAddOperations(
      allCandidates,
      sourceNode,
      sourceConnection.sourceHandle,
      { namespace, parentId },
      sourceConnection.sourceHandleType,
    );
  }, [
    editorMode.mode,
    namespace,
    nodeManager,
    parentId,
    registry,
    sourceConnection.sourceHandle,
    sourceConnection.sourceHandleType,
    sourceConnection.sourceNodeId,
  ]);

  React.useEffect(() => {
    const candidate = candidates.find(({ key }) => key === inspectedKey);
    if (candidate) {
      const nodeChanges = candidate.createChanges({
        namespace,
        parentId,
        newNodePosition,
        nodeManager,
      });
      const primaryNode = nodeChanges[0]?.item;
      if (primaryNode)
        setMenuPreview({
          connection: createConnectionFromHandles(
            {
              nodeId: sourceConnection.sourceNodeId,
              id: sourceConnection.sourceHandle,
              type: sourceConnection.sourceHandleType,
            },
            primaryNode.id,
            null,
          ),
          nodeChanges,
        });
    } else setMenuPreview(null);
    return () => setMenuPreview(null);
  }, [
    candidates,
    inspectedKey,
    namespace,
    parentId,
    newNodePosition,
    nodeManager,
    sourceConnection,
    setMenuPreview,
  ]);

  const operations = React.useMemo(() => {
    const trimmedSearch = search.trim().toLowerCase();
    if (!trimmedSearch) {
      return candidates;
    }

    return candidates.filter((operation) =>
      operation.label.toLowerCase().includes(trimmedSearch),
    );
  }, [candidates, search]);

  const title =
    sourceConnection.sourceHandleType === 'target'
      ? 'Add previous operation'
      : 'Add next operation';

  React.useEffect(() => {
    if (operations) {
      onContentChange?.();
    }
  }, [onContentChange, operations]);

  return (
    <Stack spacing={1} sx={{ px: 1.5, pt: 1.5, pb: 1.5, width: 260 }}>
      <Typography variant="subtitle2">{title}</Typography>
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
              onMouseEnter={() => setInspectedKey(operation.key)}
              onMouseLeave={() => setInspectedKey(null)}
              onFocus={() => setInspectedKey(operation.key)}
              onBlur={() => setInspectedKey(null)}
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
      {operations.length === 0 && (
        <Typography variant="body2">
          {search.trim()
            ? 'No operations match this filter.'
            : 'No operations are available here yet.'}
        </Typography>
      )}
    </Stack>
  );
}
