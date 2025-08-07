import { MarkerType } from '@xyflow/react';
import { v4 as uuidv4 } from 'uuid';
import type { DiagramEditorEdge, EdgeOutputData } from '.';
import type { DefaultEdge } from './default-edge';
import type {
  BufferKeyInputSlotData,
  BufferSeqInputSlotData,
  SectionBufferInputSlotData,
} from './input-slots';

export function createBaseEdge(
  source: string,
  target: string,
  id?: string,
): Pick<DiagramEditorEdge, 'id' | 'source' | 'target' | 'markerEnd'> {
  return {
    id: id || uuidv4(),
    source,
    target,
    markerEnd: {
      type: MarkerType.ArrowClosed,
      width: 20,
      height: 20,
    },
  };
}

export function createDefaultEdge(
  source: string,
  target: string,
  inputSlot?: DefaultEdge['data']['input'],
): DiagramEditorEdge<'default'> {
  return {
    ...createBaseEdge(source, target),
    type: 'default',
    data: { output: {}, input: inputSlot || { type: 'default' } },
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
      input: { type: 'default' },
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
    data: { output: {}, input: { type: 'default' } },
  };
}

export function createForkResultErrEdge(
  source: string,
  target: string,
): DiagramEditorEdge<'forkResultErr'> {
  return {
    ...createBaseEdge(source, target),
    type: 'forkResultErr',
    data: { output: {}, input: { type: 'default' } },
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
    data: { output: data, input: { type: 'default' } },
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
    data: { output: data, input: { type: 'default' } },
  };
}

export function createSplitRemainingEdge(
  source: string,
  target: string,
): DiagramEditorEdge<'splitRemaining'> {
  return {
    ...createBaseEdge(source, target),
    type: 'splitRemaining',
    data: { output: {}, input: { type: 'default' } },
  };
}

export function createBufferEdge(
  source: string,
  target: string,
  data:
    | BufferKeyInputSlotData
    | BufferSeqInputSlotData
    | SectionBufferInputSlotData,
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
    data: { output: data, input: { type: 'default' } },
  };
}

export function createSectionEdge(
  source: string,
  target: string,
  data: EdgeOutputData<'section'>,
): DiagramEditorEdge<'section'> {
  return {
    ...createBaseEdge(source, target),
    type: 'section',
    data: { output: data, input: { type: 'default' } },
  };
}
