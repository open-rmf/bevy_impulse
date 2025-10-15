export type DefaultInputSlotData = { type: 'default' };

export type SectionInputSlotData = {
  type: 'sectionInput';
  inputId: string;
};

export enum BufferFetchType {
  Pull,
  Clone,
}

export type BufferKeyInputSlotData = {
  type: 'bufferKey';
  key: string;
  fetch_type?: BufferFetchType;
};

export type BufferSeqInputSlotData = {
  type: 'bufferSeq';
  seq: number;
  fetch_type?: BufferFetchType;
};

export type SectionBufferInputSlotData = {
  type: 'sectionBuffer';
  inputId: string;
  fetch_type?: BufferFetchType;
};
