import type { EdgeReplaceChange } from '@xyflow/react';

import type {
  DiagramEditorEdge,
  ForkResultErrEdge,
  ForkResultOkEdge,
  UnzipEdge,
} from '../edges';
import { exhaustiveCheck } from '../utils/exhaustive-check';
import BufferEdgeForm, { type BufferEdge } from './buffer-edge-form';
import ForkResultEdgeForm from './fork-result-edge-form';
import SplitEdgeForm, { type SplitEdge } from './split-edge-form';
import UnzipEdgeForm from './unzip-edge-form';

export interface EditEdgeFormProps {
  edge: DiagramEditorEdge;
  onChange?: (change: EdgeReplaceChange<DiagramEditorEdge>) => void;
}

function EditEdgeForm({ edge, onChange }: EditEdgeFormProps) {
  switch (edge.type) {
    case 'bufferKey':
    case 'bufferSeq': {
      return (
        <BufferEdgeForm
          edge={edge as BufferEdge}
          onChange={(change) => onChange?.(change)}
        />
      );
    }
    case 'forkResultOk':
    case 'forkResultErr': {
      return (
        <ForkResultEdgeForm
          edge={edge as ForkResultOkEdge | ForkResultErrEdge}
          onChange={(change) => onChange?.(change)}
        />
      );
    }
    case 'splitKey':
    case 'splitRemaining':
    case 'splitSeq': {
      return (
        <SplitEdgeForm
          edge={edge as SplitEdge}
          onChange={(change) => onChange?.(change)}
        />
      );
    }
    case 'unzip': {
      return (
        <UnzipEdgeForm
          edge={edge as UnzipEdge}
          onChange={(change) => onChange?.(change)}
        />
      );
    }
    default: {
      return null;
    }
  }
}

export default EditEdgeForm;

export function edgeHasEditForm(edge: DiagramEditorEdge): boolean {
  switch (edge.type) {
    case 'default': {
      return false;
    }
    case 'bufferKey':
    case 'bufferSeq':
    case 'forkResultErr':
    case 'forkResultOk':
    case 'splitKey':
    case 'splitRemaining':
    case 'splitSeq':
    case 'unzip': {
      return true;
    }
    default: {
      exhaustiveCheck(edge);
      throw new Error('unknown edge');
    }
  }
}
