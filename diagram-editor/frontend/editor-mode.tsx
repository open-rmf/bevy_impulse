import {
  createContext,
  type Dispatch,
  type PropsWithChildren,
  type SetStateAction,
  useContext,
  useState,
} from 'react';

export enum EditorMode {
  Normal,
  Template,
}

export interface EditorModeNormalContext {
  mode: EditorMode.Normal;
}

export interface EditorModeTemplateContext {
  mode: EditorMode.Template;
  templateId: string;
}

export type EditorModeContext =
  | EditorModeNormalContext
  | EditorModeTemplateContext;

export type UseEditorModeContext = [
  EditorModeContext,
  Dispatch<SetStateAction<EditorModeContext>>,
];

const EditorModeContext = createContext<UseEditorModeContext | null>(null);

export function EditorModeProvider({ children }: PropsWithChildren) {
  const [mode, setMode] = useState<EditorModeContext>({
    mode: EditorMode.Normal,
  });

  return (
    <EditorModeContext.Provider value={[mode, setMode]}>
      {children}
    </EditorModeContext.Provider>
  );
}

export function useEditorMode(): UseEditorModeContext {
  const context = useContext(EditorModeContext);
  if (!context) {
    throw new Error('useEditorMode must be used within a EditorModeProvider');
  }
  return context;
}
