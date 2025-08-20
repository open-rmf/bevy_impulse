import { createContext, useContext } from 'react';
import { ApiClient } from './api-client';

const ApiClientContext = createContext<ApiClient>(new ApiClient());

export const useApiClient = () => {
  return useContext(ApiClientContext);
};
