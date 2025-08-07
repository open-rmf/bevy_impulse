import type { Edge } from '../types/react-flow';
import type { DefaultInputSlotData, SectionInputSlotData } from './input-slots';

/**
 * A specialization of `Edge` that enforces the edge input slot data is valid for data edges.
 */
export type DataEdge<
  O extends Record<string, unknown>,
  S extends string,
> = Edge<O, DefaultInputSlotData | SectionInputSlotData, S>;
