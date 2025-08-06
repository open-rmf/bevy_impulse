import { type EdgeProps, StepEdge } from '@xyflow/react';
import type { Edge } from '../types/react-flow';
import type { SectionBufferSlotData } from './section-edge';
import { memo } from 'react';

export type BufferKeySlotData = {
  type: 'bufferKey';
  key: string;
};

export type BufferSeqSlotData = {
  type: 'bufferSeq';
  seq: number;
};

export type BufferOutputData = Record<string, never>;

export type BufferEdge = Edge<
  BufferOutputData,
  BufferKeySlotData | BufferSeqSlotData | SectionBufferSlotData,
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
