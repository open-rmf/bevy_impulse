import type { Edge as ReactFlowEdge } from '@xyflow/react';

export type EdgeTypes =
  | 'default'
  | 'unzip'
  | 'forkResultOk'
  | 'forkResultErr'
  | 'splitKey'
  | 'splitSeq'
  | 'splitRemaining'
  | 'bufferKey'
  | 'bufferSeq'
  | 'streamOut';

export type Edge<
  D extends Record<string, unknown>,
  T extends EdgeTypes,
> = ReactFlowEdge<D, T> & Pick<Required<ReactFlowEdge>, 'type' | 'data'>;
