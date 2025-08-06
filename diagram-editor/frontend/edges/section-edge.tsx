import { type EdgeProps, StepEdge } from '@xyflow/react';
import { memo } from 'react';
import type { DataEdge } from '.';

export type SectionOutputData = {
  output: string;
};

export type SectionEdge = DataEdge<SectionOutputData, 'section'>;

export type SectionOutputEdgeProps = Exclude<EdgeProps<SectionEdge>, 'label'>;

export const SectionOutputEdgeComp = memo((props: SectionOutputEdgeProps) => {
  return <StepEdge {...props} label={props.data.output.output} />;
});

export type SectionInputSlotData = {
  type: 'sectionInput';
  inputId: string;
};

export type SectionBufferSlotData = {
  type: 'sectionBuffer';
  inputId: string;
};
