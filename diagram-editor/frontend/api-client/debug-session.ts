import { type Observable, Subject } from 'rxjs';
import type { DebugSessionMessage } from '../types/api';
import { getSchema } from '../utils/ajv';

const validateDebugSessionMessage = getSchema<DebugSessionMessage>(
  'DebugSessionMessage',
);

export class DebugSession {
  debugFeedback$: Observable<DebugSessionMessage>;

  constructor(ws: WebSocket) {
    const debugFeedbackSubject$ = new Subject<DebugSessionMessage>();
    ws.onmessage = (ev) => {
      try {
        const msg = JSON.parse(ev.data);
        if (!validateDebugSessionMessage(msg)) {
          console.error(validateDebugSessionMessage.errors);
          return;
        }
        debugFeedbackSubject$.next(msg);
      } catch (e) {
        console.error((e as Error).message);
      }
    };
    this.debugFeedback$ = debugFeedbackSubject$;
  }
}
