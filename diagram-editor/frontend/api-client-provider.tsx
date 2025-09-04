import { createContext, useContext } from 'react';
import { type BaseApiClient, ApiClient } from './api-client';

const ApiClientContext = createContext<BaseApiClient>(new ApiClient());

export const useApiClient = () => {
  return useContext(ApiClientContext);
};
