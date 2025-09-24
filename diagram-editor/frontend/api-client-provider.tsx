import {
  createContext,
  type JSX,
  type PropsWithChildren,
  useContext,
} from 'react';
import { ApiClient, type BaseApiClient } from './api-client';

const ApiClientContext = createContext<BaseApiClient>(new ApiClient());

export interface ApiClientProviderProps {
  value: BaseApiClient;
}

export function ApiClientProvider({
  value,
  children,
}: PropsWithChildren<ApiClientProviderProps>): JSX.Element {
  return (
    <ApiClientContext.Provider value={value}>
      {children}
    </ApiClientContext.Provider>
  );
}

export const useApiClient = () => {
  return useContext(ApiClientContext);
};
