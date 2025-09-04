import { from, type Observable, of } from 'rxjs';
import type {
  Diagram,
  DiagramElementRegistry,
  PostRunRequest,
} from '../types/api';
import { getSchema } from '../utils/ajv';
import type { ApiClient } from './base-api-client';
import { DebugSession } from './debug-session';
import * as wasmApi from './wasm/stub';

const validateRegistry = getSchema<DiagramElementRegistry>(
  'DiagramElementRegistry',
);

export class WasmClient implements ApiClient {
  getRegistry(): Observable<DiagramElementRegistry> {
    const registry = wasmApi.get_registry();
    if (!validateRegistry(registry)) {
      throw validateRegistry.errors;
    }
    return of(registry);
  }

  postRunWorkflow(diagram: Diagram, request: unknown): Observable<unknown> {
    const body: PostRunRequest = {
      diagram,
      request,
    };
    return from(wasmApi.post_run(body));
  }

  async wsDebugWorkflow(
    diagram: Diagram,
    request: unknown,
  ): Promise<DebugSession> {
    const ws = new WebSocket('/api/executor/debug');
    await new Promise((resolve, reject) => {
      ws.onopen = () => {
        const body: PostRunRequest = {
          diagram,
          request,
        };
        ws.send(JSON.stringify(body));
        resolve(ws);
      };
      ws.onerror = reject;
    });
    return new DebugSession(ws);
  }
}
