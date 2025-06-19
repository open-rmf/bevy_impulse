import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './app';

const rootEl = document.getElementById('root');
if (rootEl) {
  const root = ReactDOM.createRoot(rootEl);
  root.render(
    <React.StrictMode>
      <App />
    </React.StrictMode>,
  );
}

export { default as diagramSchema } from './diagram.schema.json';
export * from './nodes';
export * from './types';
export * from './utils';
