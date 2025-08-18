import { from, type Observable } from 'rxjs';
import type {
  Diagram,
  DiagramElementRegistry,
  PostRunRequest,
} from '../types/api';
import { getSchema } from '../utils/ajv';
import { DebugSession } from './debug-session';

const validateRegistry = getSchema<DiagramElementRegistry>(
  'DiagramElementRegistry',
);

async function getErrorMessage(response: Response) {
  const text = await response.text();
  return text || `${response.status} ${response.statusText}`;
}

export class ApiClient {
  getRegistry(): Observable<DiagramElementRegistry> {
    return from(
      (async () => {
        const response = await fetch('/api/registry');
        if (!response.ok) {
          throw new Error(await getErrorMessage(response));
        }
        const data = await response.json();
        if (!validateRegistry(data)) {
          throw validateRegistry.errors;
        }
        return data;
      })(),
    );
  }

  postRunWorkflow(diagram: Diagram, request: unknown): Observable<unknown> {
    return from(
      (async () => {
        const body: PostRunRequest = {
          diagram,
          request,
        };
        const response = await fetch('/api/executor/run', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(body),
        });
        if (!response.ok) {
          throw new Error(await getErrorMessage(response));
        }
        return response.json();
      })(),
    );
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
