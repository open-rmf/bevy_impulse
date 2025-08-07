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
  return (
    <StepEdge
      {...props}
      label={
        props.data.input.type === 'sectionBuffer'
          ? props.data.input.inputId
          : undefined
      }
    />
  );
});
