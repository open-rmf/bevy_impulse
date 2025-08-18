import { createContext, useContext } from 'react';
import type { DiagramEditorEdge } from './edges';

const EdgesContext = createContext<DiagramEditorEdge[]>([]);

export const EdgesProvider = EdgesContext.Provider;

export function useEdges() {
  return useContext(EdgesContext);
}
