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
import type { DiagramElementRegistry } from './types/api';
import { getSchema } from './utils/ajv';

const validateRegistry = getSchema<DiagramElementRegistry>(
  'DiagramElementRegistry',
);

const RegistryContextComp = createContext<DiagramElementRegistry | null>(null);

export const RegistryProvider = ({ children }: PropsWithChildren) => {
  const [registry, setRegistry] = useState<DiagramElementRegistry | null>(null);
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

  if (!registry) {
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
    <RegistryContextComp.Provider value={registry}>
      {children}
    </RegistryContextComp.Provider>
  );
};

export const useRegistry = () => {
  const context = useContext(RegistryContextComp);
  if (!context) {
    throw new Error('useRegistry must be used within a RegistryProvider');
  }
  return context;
};
