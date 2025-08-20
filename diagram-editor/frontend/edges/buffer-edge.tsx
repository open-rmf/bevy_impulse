import { type EdgeProps, StepEdge } from '@xyflow/react';
import { memo } from 'react';
import type { Edge } from '../types/react-flow';
import type {
  BufferKeyInputSlotData,
  BufferSeqInputSlotData,
  SectionBufferInputSlotData,
} from './input-slots';

export type BufferOutputData = Record<string, never>;

export type BufferEdge = Edge<
  BufferOutputData,
  BufferKeyInputSlotData | BufferSeqInputSlotData | SectionBufferInputSlotData,
  'buffer'
>;

export type BufferEdgeCompProps = Exclude<EdgeProps<BufferEdge>, 'label'>;

export const BufferEdgeComp = memo((props: BufferEdgeCompProps) => {
  const label = (() => {
    switch (props.data.input.type) {
      case 'bufferKey': {
        return props.data.input.key || 'Select Key';
      }
      case 'bufferSeq': {
        return props.data.input.seq.toString();
      }
      case 'sectionBuffer': {
        return props.data.input.inputId || 'Select Buffer';
      }
    }
  })();

  return <StepEdge {...props} label={label} />;
});
