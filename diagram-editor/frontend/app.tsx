import '@fontsource/roboto/300.css';
import '@fontsource/roboto/400.css';
import '@fontsource/roboto/500.css';
import '@fontsource/roboto/700.css';
import '@xyflow/react/dist/style.css';
import { CssBaseline, createTheme, ThemeProvider } from '@mui/material';

import './app.css';
import DiagramEditor from './diagram-editor';
import { EditorModeProvider } from './editor-mode';
import { RegistryProvider } from './registry-provider';
import { TemplatesProvider } from './templates-provider';

const theme = createTheme({
  palette: {
    mode: 'dark',
  },
});

const App = () => {
  return (
    <ThemeProvider theme={theme}>
      <CssBaseline enableColorScheme />
      <div style={{ width: '100vw', height: '100vh' }}>
        <RegistryProvider>
          <TemplatesProvider>
            <EditorModeProvider>
              <DiagramEditor />
            </EditorModeProvider>
          </TemplatesProvider>
        </RegistryProvider>
      </div>
    </ThemeProvider>
  );
};

export default App;
