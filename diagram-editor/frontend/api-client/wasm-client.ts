import { from, type Observable } from 'rxjs';
import type {
  CompatibilityRequest,
  CompatibilityResponse,
  Diagram,
  DiagramElementMetadata,
} from '../types/api';
import { getSchema } from '../utils/ajv';
import type { BaseApiClient } from './base-api-client';
import * as wasmApi from './wasm-stub/stub.js';

const validateRegistry = getSchema<DiagramElementMetadata>(
  'DiagramElementMetadata',
);

export class ApiClient implements BaseApiClient {
  constructor() {
    wasmApi.init_wasm();
  }

  getRegistry(): Observable<DiagramElementMetadata> {
    return from(
      (async () => {
        const registry = wasmApi.get_registry();
        if (!validateRegistry(registry)) {
          throw validateRegistry.errors;
        }
        return registry;
      })(),
    );
  }

  postRunWorkflow(diagram: Diagram, request: unknown): Observable<unknown> {
    return from(
      wasmApi.post_run(new wasmApi.PostRunRequestWasm({ diagram, request })),
    );
  }

  checkCompatibility(
    request: CompatibilityRequest,
  ): Observable<CompatibilityResponse> {
    // Match REST JSON serialization, which omits undefined optional fields.
    return from(
      wasmApi.check_compatibility(
        new wasmApi.CompatibilityRequestWasm(
          JSON.parse(JSON.stringify(request)),
        ),
      ),
    );
  }
}
