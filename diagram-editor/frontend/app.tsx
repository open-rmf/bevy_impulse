import '@fontsource/roboto/300.css';
import '@fontsource/roboto/400.css';
import '@fontsource/roboto/500.css';
import '@fontsource/roboto/700.css';
import '@xyflow/react/dist/style.css';
import { CssBaseline, ThemeProvider, createTheme } from '@mui/material';

import './app.css';
import DiagramEditor from './diagram-editor';

const theme = createTheme({
  palette: {
    mode: 'dark',
  },
});

const App = () => {
  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <div style={{ width: '100vw', height: '100vh' }}>
        <DiagramEditor />
      </div>
    </ThemeProvider>
  );
};

export default App;
