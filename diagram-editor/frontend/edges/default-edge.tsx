import { type EdgeProps, StepEdge } from '@xyflow/react';
import { memo } from 'react';
import type { DataEdge } from '.';

export type DefaultOutputData = Record<string, never>;

export type DefaultEdge = DataEdge<DefaultOutputData, 'default'>;

export type DefaultEdgeCompProps = Omit<EdgeProps<DefaultEdge>, 'label'>;

export const DefaultEdgeComp = memo((props: DefaultEdgeCompProps) => {
  return (
    <StepEdge
      {...props}
      label={
        props.data.input.type === 'sectionInput'
          ? props.data.input.inputId
          : undefined
      }
    />
  );
});
