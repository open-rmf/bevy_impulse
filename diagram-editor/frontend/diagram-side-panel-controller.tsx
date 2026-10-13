import {
  createContext,
  type PropsWithChildren,
  useCallback,
  useContext,
  useMemo,
  useRef,
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

type EnvironmentLeaveGuard = (proceed: () => void, hidingOnly: boolean) => void;

export interface DiagramSidePanelController {
  state: DiagramSidePanelState;
  restoreState: (state: DiagramSidePanelState) => void;
  close: () => void;
  setEnvironmentLeaveGuard: (guard: EnvironmentLeaveGuard | null) => void;
  requestEnvironmentLeave: (proceed: () => void, hidingOnly?: boolean) => void;
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

  const leaveGuard = useRef<EnvironmentLeaveGuard | null>(null);
  const setEnvironmentLeaveGuard = useCallback(
    (guard: EnvironmentLeaveGuard | null) => {
      leaveGuard.current = guard;
    },
    [],
  );
  const requestEnvironmentLeave = useCallback(
    (proceed: () => void, hidingOnly = false) => {
      if (leaveGuard.current) leaveGuard.current(proceed, hidingOnly);
      else proceed();
    },
    [],
  );

  const close = useCallback(
    () =>
      requestEnvironmentLeave(() => {
        setState((prev) => ({
          ...prev,
          open: false,
          mode: 'view',
          expanded: false,
        }));
      }, true),
    [requestEnvironmentLeave],
  );

  const showTab = useCallback(
    (tab: DiagramSidePanelTab) => {
      const show = () =>
        setState((prev) => ({
          ...prev,
          open: true,
          tab,
          mode: tab === 'environments' ? prev.mode : 'view',
          expanded: tab === 'environments' ? prev.expanded : false,
        }));
      if (tab === 'environments') show();
      else requestEnvironmentLeave(show, true);
    },
    [requestEnvironmentLeave],
  );

  const toggleTab = useCallback(
    (tab: DiagramSidePanelTab) => {
      if (state.open && state.tab === tab) close();
      else showTab(tab);
    },
    [close, showTab, state.open, state.tab],
  );

  const openEnvironments = useCallback(
    ({ environmentName, create = false }: OpenEnvironmentsOptions = {}) => {
      requestEnvironmentLeave(() =>
        setState((prev) => ({
          ...prev,
          open: true,
          tab: 'environments',
          selectedEnvironmentName: environmentName,
          mode: create ? 'create' : 'view',
          expanded: create,
          width: undefined,
        })),
      );
    },
    [requestEnvironmentLeave],
  );

  const selectEnvironment = useCallback(
    (name?: string) => {
      requestEnvironmentLeave(() =>
        setState((prev) => ({ ...prev, selectedEnvironmentName: name })),
      );
    },
    [requestEnvironmentLeave],
  );

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
    leaveGuard.current = null;
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
      restoreState: setState,
      setEnvironmentLeaveGuard,
      requestEnvironmentLeave,
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
      setEnvironmentLeaveGuard,
      requestEnvironmentLeave,
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
