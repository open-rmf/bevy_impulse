import { MarkerType } from '@xyflow/react';
import { v4 as uuidv4 } from 'uuid';
import type { DiagramEditorEdge, EdgeOutputData } from '.';
import type { BufferKeySlotData, BufferSeqSlotData } from './buffer-edge';

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
    data: { output: {} },
  };
}

export function createUnzipEdge(
  source: string,
  target: string,
  data: EdgeOutputData<'unzip'>,
): DiagramEditorEdge<'unzip'> {
  return {
    ...createBaseEdge(source, target),
    type: 'unzip',
    data: {
      output: data,
    },
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
  data: EdgeOutputData<'splitKey'>,
): DiagramEditorEdge<'splitKey'> {
  return {
    ...createBaseEdge(source, target),
    type: 'splitKey',
    data: { output: data },
  };
}

export function createSplitSeqEdge(
  source: string,
  target: string,
  data: EdgeOutputData<'splitSeq'>,
): DiagramEditorEdge<'splitSeq'> {
  return {
    ...createBaseEdge(source, target),
    type: 'splitSeq',
    data: { output: data },
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

export function createBufferEdge(
  source: string,
  target: string,
  data: BufferKeySlotData | BufferSeqSlotData,
): DiagramEditorEdge<'buffer'> {
  return {
    ...createBaseEdge(source, target),
    type: 'buffer',
    data: { output: {}, input: data },
  };
}

export function createStreamOutEdge(
  source: string,
  target: string,
  data: EdgeOutputData<'streamOut'>,
): DiagramEditorEdge<'streamOut'> {
  return {
    ...createBaseEdge(source, target),
    type: 'streamOut',
    data: { output: data },
  };
}

// export function createSectionEdge(
//   source: string,
//   target: string,
//   data: EdgeOutputData<'section'>,
// ): DiagramEditorEdge<'section'> {
//   return {
//     ...createBaseEdge(source, target),
//     type: 'section',
//     data: { output: data },
//   };
// }
