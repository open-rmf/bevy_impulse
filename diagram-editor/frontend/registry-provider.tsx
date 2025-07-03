import { Box, CircularProgress, Typography } from '@mui/material';
import {
  createContext,
  type PropsWithChildren,
  useContext,
  useEffect,
  useState,
} from 'react';
import { from, timer } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import registrySchema from './registry.preprocessed.schema.json';
import type { DiagramElementRegistry } from './types';
import ajv from './utils/ajv';

const validateRegistry = ajv.compile<DiagramElementRegistry>(registrySchema);

type RegistryContextValue = {
  registry: DiagramElementRegistry | null;
  loading: boolean;
  error: Error | null;
};

const RegistryContext = createContext<RegistryContextValue | null>(null);

export const RegistryProvider = ({ children }: PropsWithChildren) => {
  const [registry, setRegistry] = useState<DiagramElementRegistry | null>(null);
  const [loading, setLoading] = useState(true);
  const [showLoading, setShowLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    const fetch$ = from(
      (async () => {
        const response = await fetch('/api/registry');
        if (!response.ok) {
          throw new Error(`Failed to fetch registry: ${response.statusText}`);
        }
        const data = await response.json();
        if (!validateRegistry(data)) {
          throw validateRegistry.errors;
        }
        return data;
      })(),
    );

    const timer$ = timer(1000);

    const timerSubscription = timer$.pipe(takeUntil(fetch$)).subscribe(() => {
      setShowLoading(true);
    });

    const fetchSubscription = fetch$.subscribe({
      next: setRegistry,
      error: (err) => {
        console.error(err);
        setError(err as Error);
      },
      complete: () => {
        setLoading(false);
      },
    });

    return () => {
      timerSubscription.unsubscribe();
      fetchSubscription.unsubscribe();
    };
  }, []);

  if (error) {
    return (
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          height: '100vh',
        }}
      >
        <Typography>Failed to fetch registry</Typography>
      </Box>
    );
  }

  if (loading) {
    if (showLoading) {
      return (
        <Box
          sx={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            height: '100vh',
          }}
        >
          <CircularProgress />
        </Box>
      );
    }
    return null;
  }

  return (
    <RegistryContext.Provider value={{ registry, loading, error }}>
      {children}
    </RegistryContext.Provider>
  );
};

export const useRegistry = () => {
  const context = useContext(RegistryContext);
  if (!context) {
    throw new Error('useRegistry must be used within a RegistryProvider');
  }
  return context;
};
