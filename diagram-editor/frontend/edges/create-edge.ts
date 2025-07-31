import { MarkerType } from '@xyflow/react';
import { v4 as uuidv4 } from 'uuid';
import type { DiagramEditorEdge, EdgeData } from '.';

export function createBaseEdge(
  source: string,
  target: string,
): Pick<DiagramEditorEdge, 'id' | 'source' | 'target' | 'markerEnd'> {
  return {
    id: uuidv4(),
    source,
    target,
    markerEnd: {
      type: MarkerType.ArrowClosed,
      width: 24,
      height: 24,
    },
  };
}

export function createDefaultEdge(
  source: string,
  target: string,
): DiagramEditorEdge<'default'> {
  return {
    ...createBaseEdge(source, target),
    type: 'default',
    data: {},
  };
}

export function createUnzipEdge(
  source: string,
  target: string,
  data: EdgeData<'unzip'>,
): DiagramEditorEdge<'unzip'> {
  return {
    ...createBaseEdge(source, target),
    type: 'unzip',
    data,
  };
}

export function createForkResultOkEdge(
  source: string,
  target: string,
): DiagramEditorEdge<'forkResultOk'> {
  return {
    ...createBaseEdge(source, target),
    type: 'forkResultOk',
    data: {},
  };
}

export function createForkResultErrEdge(
  source: string,
  target: string,
): DiagramEditorEdge<'forkResultErr'> {
  return {
    ...createBaseEdge(source, target),
    type: 'forkResultErr',
    data: {},
  };
}

export function createSplitKeyEdge(
  source: string,
  target: string,
  data: EdgeData<'splitKey'>,
): DiagramEditorEdge<'splitKey'> {
  return {
    ...createBaseEdge(source, target),
    type: 'splitKey',
    data,
  };
}

export function createSplitSeqEdge(
  source: string,
  target: string,
  data: EdgeData<'splitSeq'>,
): DiagramEditorEdge<'splitSeq'> {
  return {
    ...createBaseEdge(source, target),
    type: 'splitSeq',
    data,
  };
}

export function createSplitRemainingEdge(
  source: string,
  target: string,
): DiagramEditorEdge<'splitRemaining'> {
  return {
    ...createBaseEdge(source, target),
    type: 'splitRemaining',
    data: {},
  };
}

export function createBufferKeyEdge(
  source: string,
  target: string,
  data: EdgeData<'bufferKey'>,
): DiagramEditorEdge<'bufferKey'> {
  return {
    ...createBaseEdge(source, target),
    type: 'bufferKey',
    data,
  };
}

export function createBufferSeqEdge(
  source: string,
  target: string,
  data: EdgeData<'bufferSeq'>,
): DiagramEditorEdge<'bufferSeq'> {
  return {
    ...createBaseEdge(source, target),
    type: 'bufferSeq',
    data,
  };
}

export function createStreamOutEdge(
  source: string,
  target: string,
  data: EdgeData<'streamOut'>,
): DiagramEditorEdge<'streamOut'> {
  return {
    ...createBaseEdge(source, target),
    type: 'streamOut',
    data,
  };
}
