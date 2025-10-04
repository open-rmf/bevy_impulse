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
  sourceHandle: string | null | undefined,
  target: string,
  targetHandle: string | null | undefined,
  id?: string,
): Pick<
  DiagramEditorEdge,
  'id' | 'source' | 'target' | 'markerEnd' | 'sourceHandle' | 'targetHandle'
> {
  return {
    id: id || uuidv4(),
    source,
    sourceHandle,
    target,
    targetHandle,
    markerEnd: {
      type: MarkerType.ArrowClosed,
      width: 20,
      height: 20,
    },
  };
}

export function createDefaultEdge(
  source: string,
  sourceHandle: string | null | undefined,
  target: string,
  targetHandle: string | null | undefined,
  inputSlot?: DefaultEdge['data']['input'],
): DiagramEditorEdge<'default'> {
  return {
    ...createBaseEdge(source, sourceHandle, target, targetHandle),
    type: 'default',
    data: { output: {}, input: inputSlot || { type: 'default' } },
  };
}

export function createUnzipEdge(
  source: string,
  sourceHandle: string | null | undefined,
  target: string,
  targetHandle: string | null | undefined,
  data: EdgeOutputData<'unzip'>,
): DiagramEditorEdge<'unzip'> {
  return {
    ...createBaseEdge(source, sourceHandle, target, targetHandle),
    type: 'unzip',
    data: {
      output: data,
      input: { type: 'default' },
    },
  };
}

export function createForkResultOkEdge(
  source: string,
  sourceHandle: string | null | undefined,
  target: string,
  targetHandle: string | null | undefined,
): DiagramEditorEdge<'forkResultOk'> {
  return {
    ...createBaseEdge(source, sourceHandle, target, targetHandle),
    type: 'forkResultOk',
    data: { output: {}, input: { type: 'default' } },
  };
}

export function createForkResultErrEdge(
  source: string,
  sourceHandle: string | null | undefined,
  target: string,
  targetHandle: string | null | undefined,
): DiagramEditorEdge<'forkResultErr'> {
  return {
    ...createBaseEdge(source, sourceHandle, target, targetHandle),
    type: 'forkResultErr',
    data: { output: {}, input: { type: 'default' } },
  };
}

export function createSplitKeyEdge(
  source: string,
  sourceHandle: string | null | undefined,
  target: string,
  targetHandle: string | null | undefined,
  data: EdgeOutputData<'splitKey'>,
): DiagramEditorEdge<'splitKey'> {
  return {
    ...createBaseEdge(source, sourceHandle, target, targetHandle),
    type: 'splitKey',
    data: { output: data, input: { type: 'default' } },
  };
}

export function createSplitSeqEdge(
  source: string,
  sourceHandle: string | null | undefined,
  target: string,
  targetHandle: string | null | undefined,
  data: EdgeOutputData<'splitSeq'>,
): DiagramEditorEdge<'splitSeq'> {
  return {
    ...createBaseEdge(source, sourceHandle, target, targetHandle),
    type: 'splitSeq',
    data: { output: data, input: { type: 'default' } },
  };
}

export function createSplitRemainingEdge(
  source: string,
  sourceHandle: string | null | undefined,
  target: string,
  targetHandle: string | null | undefined,
): DiagramEditorEdge<'splitRemaining'> {
  return {
    ...createBaseEdge(source, sourceHandle, target, targetHandle),
    type: 'splitRemaining',
    data: { output: {}, input: { type: 'default' } },
  };
}

export function createBufferEdge(
  source: string,
  sourceHandle: string | null | undefined,
  target: string,
  targetHandle: string | null | undefined,
  data:
    | BufferKeyInputSlotData
    | BufferSeqInputSlotData
    | SectionBufferInputSlotData,
): DiagramEditorEdge<'buffer'> {
  return {
    ...createBaseEdge(source, sourceHandle, target, targetHandle),
    type: 'buffer',
    data: { output: {}, input: data },
  };
}

export function createStreamOutEdge(
  source: string,
  sourceHandle: string | null | undefined,
  target: string,
  targetHandle: string | null | undefined,
  data: EdgeOutputData<'streamOut'>,
): DiagramEditorEdge<'streamOut'> {
  return {
    ...createBaseEdge(source, sourceHandle, target, targetHandle),
    type: 'streamOut',
    data: { output: data, input: { type: 'default' } },
  };
}

export function createSectionEdge(
  source: string,
  sourceHandle: string | null | undefined,
  target: string,
  targetHandle: string | null | undefined,
  data: EdgeOutputData<'section'>,
): DiagramEditorEdge<'section'> {
  return {
    ...createBaseEdge(source, sourceHandle, target, targetHandle),
    type: 'section',
    data: { output: data, input: { type: 'default' } },
  };
}
