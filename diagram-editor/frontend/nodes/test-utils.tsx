import {
  render as baseRender,
  type RenderOptions,
} from '@testing-library/react';
import { type NodeProps, Position, ReactFlowProvider } from '@xyflow/react';
import type { PropsWithChildren, ReactElement } from 'react';
import { of } from 'rxjs';
import { ApiClient } from '../api-client';
import { ApiClientProvider } from '../api-client-provider';
import { RegistryProvider } from '../registry-provider';
import type { DiagramElementRegistry } from '../types/api';
import type { OperationNode, OperationNodeTypes } from '.';

export function createOperationNodeProps<
  NodeType extends OperationNodeTypes = OperationNodeTypes,
>(operationNode: OperationNode<NodeType>): NodeProps<OperationNode<NodeType>> {
  return {
    id: operationNode.id,
    data: operationNode.data,
    type: operationNode.type,
    selected: false,
    isConnectable: true,
    dragging: false,
    draggable: true,
    selectable: true,
    deletable: true,
    positionAbsoluteX: 0,
    positionAbsoluteY: 0,
    targetPosition: Position.Top,
    sourcePosition: Position.Bottom,
    zIndex: 0,
    width: operationNode.width,
    height: operationNode.height,
  };
}

function createTestingProviders(registry: DiagramElementRegistry) {
  return ({ children }: PropsWithChildren) => {
    const apiClient = new ApiClient();
    jest.spyOn(apiClient, 'getRegistry').mockReturnValue(of(registry));

    return (
      <ApiClientProvider value={apiClient}>
        <RegistryProvider>
          <ReactFlowProvider>{children}</ReactFlowProvider>
        </RegistryProvider>
      </ApiClientProvider>
    );
  };
}

export function render(
  ui: ReactElement,
  registry?: DiagramElementRegistry,
  options?: Omit<RenderOptions, 'wrapper'>,
) {
  registry = registry || {
    messages: {},
    nodes: {},
    schemas: {},
    sections: {},
    trace_supported: false,
  };
  return baseRender(ui, {
    wrapper: createTestingProviders(registry),
    ...options,
  });
}
