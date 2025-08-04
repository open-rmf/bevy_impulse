import { type EdgeProps, StepEdge } from '@xyflow/react';
import { memo } from 'react';
import type { Edge } from '../types/react-flow';
import type { EdgeData } from '.';

export type SectionOutputData = {
  output: string;
};

export type SectionEdge = Edge<EdgeData<SectionOutputData>, 'section'>;

export type SectionOutputEdgeProps = Exclude<EdgeProps<SectionEdge>, 'label'>;

export const SectionOutputEdgeComp = memo((props: SectionOutputEdgeProps) => {
  return <StepEdge {...props} label={props.data.output.toString()} />;
});

export type SectionInputSlotData = {
  type: 'sectionInput';
  input: string;
};

export type SectionBufferSlotData = {
  type: 'sectionBuffer';
  input: string;
};
