import { createContext, useContext } from 'react';
import { type ApiClient, createApiClient } from './api-client';

const ApiClientContext = createContext<ApiClient>(createApiClient());

export const useApiClient = () => {
  return useContext(ApiClientContext);
};
