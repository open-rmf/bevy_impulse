export type DefaultInputSlotData = { type: 'default' };

export type SectionInputSlotData = {
  type: 'sectionInput';
  inputId: string;
};

export enum BufferPullType {
  Pull,
  Clone,
}

export type BufferKeyInputSlotData = {
  type: 'bufferKey';
  key: string;
  pull_type?: BufferPullType;
};

export type BufferSeqInputSlotData = {
  type: 'bufferSeq';
  seq: number;
  pull_type?: BufferPullType;
};

export type SectionBufferInputSlotData = {
  type: 'sectionBuffer';
  inputId: string;
  pull_type?: BufferPullType;
};
