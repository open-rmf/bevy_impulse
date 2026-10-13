import createCache from '@emotion/cache';
import { CacheProvider } from '@emotion/react';
import { ThemeProvider, useTheme } from '@mui/material';
import {
  type ReactNode,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import { createPortal } from 'react-dom';

// Keep the portal target stable so moving windows does not remount CodeMirror.
export function EnvironmentWindow({
  popup,
  children,
}: {
  popup: Window | null;
  children: ReactNode;
}) {
  const [host] = useState(() => document.createElement('div'));
  const anchor = useRef<HTMLDivElement>(null);
  const theme = useTheme();
  const target = popup?.document || document;
  const cache = useMemo(
    () => createCache({ key: 'environment', container: target.head }),
    [target],
  );
  useLayoutEffect(() => {
    (popup?.document.body || anchor.current)?.appendChild(host);
    if (!popup) return;
    popup.document.title = 'Crossflow — Environment';
    const base = popup.document.createElement('base');
    base.href = document.baseURI;
    const styles = Array.from(document.styleSheets, (sheet) => {
      if (sheet.href) {
        const link = popup.document.createElement('link');
        link.rel = 'stylesheet';
        link.href = sheet.href;
        return link;
      }
      const style = popup.document.createElement('style');
      style.textContent = Array.from(
        sheet.cssRules,
        (rule) => rule.cssText,
      ).join('\n');
      return style;
    });
    popup.document.head.append(base, ...styles);
    return () => {
      base.remove();
      for (const style of styles) style.remove();
    };
  }, [popup, host]);
  return (
    <>
      <div ref={anchor} />
      {createPortal(
        <CacheProvider value={cache}>
          <ThemeProvider
            theme={{
              ...theme,
              components: {
                ...theme.components,
                MuiPopover: { defaultProps: { container: target.body } },
                MuiPopper: { defaultProps: { container: target.body } },
                MuiModal: { defaultProps: { container: target.body } },
              },
            }}
          >
            {children}
          </ThemeProvider>
        </CacheProvider>,
        host,
      )}
    </>
  );
}
