import { from, type Observable } from 'rxjs';
import type {
  Diagram,
  DiagramElementRegistry,
  PostRunRequest,
} from './types/api';
import { getSchema } from './utils/ajv';

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
}
