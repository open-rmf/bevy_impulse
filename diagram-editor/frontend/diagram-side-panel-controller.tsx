import {
  createContext,
  type PropsWithChildren,
  useCallback,
  useContext,
  useMemo,
  useState,
} from 'react';

export type DiagramSidePanelTab = 'properties' | 'run' | 'environments';
export type EnvironmentPanelMode = 'view' | 'create' | 'edit';

export interface DiagramSidePanelState {
  open: boolean;
  tab: DiagramSidePanelTab;
  selectedEnvironmentName?: string;
  mode: EnvironmentPanelMode;
  expanded: boolean;
  width?: number;
}

export interface OpenEnvironmentsOptions {
  environmentName?: string;
  create?: boolean;
}

export interface DiagramSidePanelController {
  state: DiagramSidePanelState;
  close: () => void;
  showTab: (tab: DiagramSidePanelTab) => void;
  toggleTab: (tab: DiagramSidePanelTab) => void;
  openEnvironments: (options?: OpenEnvironmentsOptions) => void;
  selectEnvironment: (name?: string) => void;
  startEnvironmentCreate: () => void;
  startEnvironmentEdit: () => void;
  finishEnvironmentEdit: () => void;
  setExpanded: (expanded: boolean) => void;
  setWidth: (width: number) => void;
}

const InitialState: DiagramSidePanelState = {
  open: true,
  tab: 'properties',
  mode: 'view',
  expanded: false,
};

const DiagramSidePanelContext =
  createContext<DiagramSidePanelController | null>(null);

export function DiagramSidePanelProvider({ children }: PropsWithChildren) {
  const [state, setState] = useState(InitialState);

  const close = useCallback(() => {
    setState((prev) => ({
      ...prev,
      open: false,
      mode: 'view',
      expanded: false,
    }));
  }, []);

  const showTab = useCallback((tab: DiagramSidePanelTab) => {
    setState((prev) => ({
      ...prev,
      open: true,
      tab,
      mode: tab === 'environments' ? prev.mode : 'view',
      expanded: tab === 'environments' ? prev.expanded : false,
    }));
  }, []);

  const toggleTab = useCallback((tab: DiagramSidePanelTab) => {
    setState((prev) => {
      if (prev.open && prev.tab === tab) {
        return {
          ...prev,
          open: false,
          mode: 'view',
          expanded: false,
        };
      }

      return {
        ...prev,
        open: true,
        tab,
        mode: tab === 'environments' ? prev.mode : 'view',
        expanded: tab === 'environments' ? prev.expanded : false,
      };
    });
  }, []);

  const openEnvironments = useCallback(
    ({ environmentName, create = false }: OpenEnvironmentsOptions = {}) => {
      setState((prev) => ({
        ...prev,
        open: true,
        tab: 'environments',
        selectedEnvironmentName: environmentName,
        mode: create ? 'create' : 'view',
        expanded: create,
        width: undefined,
      }));
    },
    [],
  );

  const selectEnvironment = useCallback((name?: string) => {
    setState((prev) => ({
      ...prev,
      selectedEnvironmentName: name,
    }));
  }, []);

  const startEnvironmentCreate = useCallback(() => {
    setState((prev) => ({
      ...prev,
      open: true,
      tab: 'environments',
      selectedEnvironmentName: undefined,
      mode: 'create',
      expanded: true,
      width: undefined,
    }));
  }, []);

  const startEnvironmentEdit = useCallback(() => {
    setState((prev) => ({
      ...prev,
      open: true,
      tab: 'environments',
      mode: 'edit',
      expanded: true,
      width: undefined,
    }));
  }, []);

  const finishEnvironmentEdit = useCallback(() => {
    setState((prev) => ({
      ...prev,
      mode: 'view',
      expanded: false,
      width: undefined,
    }));
  }, []);

  const setExpanded = useCallback((expanded: boolean) => {
    setState((prev) => ({
      ...prev,
      expanded,
      width: undefined,
    }));
  }, []);

  const setWidth = useCallback((width: number) => {
    setState((prev) => ({ ...prev, width }));
  }, []);

  const value = useMemo<DiagramSidePanelController>(
    () => ({
      state,
      close,
      showTab,
      toggleTab,
      openEnvironments,
      selectEnvironment,
      startEnvironmentCreate,
      startEnvironmentEdit,
      finishEnvironmentEdit,
      setExpanded,
      setWidth,
    }),
    [
      close,
      finishEnvironmentEdit,
      openEnvironments,
      selectEnvironment,
      setExpanded,
      setWidth,
      showTab,
      startEnvironmentCreate,
      startEnvironmentEdit,
      state,
      toggleTab,
    ],
  );

  return (
    <DiagramSidePanelContext.Provider value={value}>
      {children}
    </DiagramSidePanelContext.Provider>
  );
}

export function useDiagramSidePanel(): DiagramSidePanelController {
  const context = useContext(DiagramSidePanelContext);
  if (!context) {
    throw new Error(
      'useDiagramSidePanel must be used within a DiagramSidePanelProvider',
    );
  }
  return context;
}
