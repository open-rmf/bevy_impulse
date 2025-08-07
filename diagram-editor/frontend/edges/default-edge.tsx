import { type EdgeProps, StepEdge } from '@xyflow/react';
import { memo } from 'react';
import type { DataEdge } from '.';

export type DefaultEdgeOutputData = Record<string, never>;

export type DefaultEdge = DataEdge<DefaultEdgeOutputData, 'default'>;

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
