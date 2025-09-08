import { createContext, useContext } from 'react';
import { ApiClient, type BaseApiClient } from './api-client';

const ApiClientContext = createContext<BaseApiClient>(new ApiClient());

export const useApiClient = () => {
  return useContext(ApiClientContext);
};
