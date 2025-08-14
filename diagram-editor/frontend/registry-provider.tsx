import { Box, CircularProgress, Typography } from '@mui/material';
import {
  createContext,
  type PropsWithChildren,
  useContext,
  useEffect,
  useState,
} from 'react';
import { timer } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { useApiClient } from './api-client-provider';
import type { DiagramElementRegistry } from './types/api';

const RegistryContextComp = createContext<DiagramElementRegistry | null>(null);

export const RegistryProvider = ({ children }: PropsWithChildren) => {
  const [registry, setRegistry] = useState<DiagramElementRegistry | null>(null);
  const [showLoading, setShowLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const apiClient = useApiClient();

  useEffect(() => {
    const registry$ = apiClient.getRegistry();

    const timer$ = timer(1000);

    const timerSubscription = timer$
      .pipe(takeUntil(registry$))
      .subscribe(() => {
        setShowLoading(true);
      });

    const fetchSubscription = registry$.subscribe({
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
  }, [apiClient]);

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
