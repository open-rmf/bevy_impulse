import React, { createContext, type Dispatch, useContext } from 'react';

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
  Dispatch<EditorModeContext>,
];

const EditorModeContextComp = createContext<UseEditorModeContext | null>(null);

export function EditorModeProvider({
  value,
  children,
}: React.ProviderProps<UseEditorModeContext>) {
  return (
    <EditorModeContextComp.Provider value={value}>
      {children}
    </EditorModeContextComp.Provider>
  );
}

export function useEditorMode(): UseEditorModeContext {
  const context = useContext(EditorModeContextComp);
  if (!context) {
    throw new Error('useEditorMode must be used within a EditorModeProvider');
  }
  return context;
}
