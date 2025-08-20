export type DefaultInputSlotData = { type: 'default' };

export type SectionInputSlotData = {
  type: 'sectionInput';
  inputId: string;
};

export type BufferKeyInputSlotData = {
  type: 'bufferKey';
  key: string;
};

export type BufferSeqInputSlotData = {
  type: 'bufferSeq';
  seq: number;
};

export type SectionBufferInputSlotData = {
  type: 'sectionBuffer';
  inputId: string;
};
