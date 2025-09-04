import type { Observable } from 'rxjs';
import type { Diagram, DiagramElementRegistry } from '../types/api';

export interface BaseApiClient {
  getRegistry(): Observable<DiagramElementRegistry>;
  postRunWorkflow(diagram: Diagram, request: unknown): Observable<unknown>;
  // WIP
  // wsDebugWorkflow(diagram: Diagram, request: unknown): Promise<DebugSession>;
}
