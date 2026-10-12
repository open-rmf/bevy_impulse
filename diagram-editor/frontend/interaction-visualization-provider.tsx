import { createContext, type PropsWithChildren, useContext } from 'react';

export interface InteractionVisualizationContext {
  isWorkflowRunning: boolean;
  setWorkflowRunning: React.Dispatch<React.SetStateAction<boolean>>;
  activeNodeIds: Set<string>;
  visitedNodeIds: Set<string>;
  clearInteractionVisualization: () => void;
  markInteractionFinished: () => void;
  markInteractionOperationFinished: (operationId: string) => void;
  markInteractionOperationStarted: (operationId: string) => void;
}

const DefaultInteractionVisualizationContext: InteractionVisualizationContext =
  {
    isWorkflowRunning: false,
    setWorkflowRunning: () => {},
    activeNodeIds: new Set(),
    visitedNodeIds: new Set(),
    clearInteractionVisualization: () => {},
    markInteractionFinished: () => {},
    markInteractionOperationFinished: () => {},
    markInteractionOperationStarted: () => {},
  };

const InteractionVisualizationContextComp =
  createContext<InteractionVisualizationContext>(
    DefaultInteractionVisualizationContext,
  );

export function InteractionVisualizationProvider({
  value,
  children,
}: PropsWithChildren<{ value: InteractionVisualizationContext }>) {
  return (
    <InteractionVisualizationContextComp.Provider value={value}>
      {children}
    </InteractionVisualizationContextComp.Provider>
  );
}

export function useInteractionVisualization() {
  return useContext(InteractionVisualizationContextComp);
}
