import type { ApiClient } from './base-api-client';
import { RestClient } from './rest-client';

export function createApiClient(): ApiClient {
  // TODO: create wasm client based on compiler option
  return new RestClient();
}
