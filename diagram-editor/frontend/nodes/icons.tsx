import TransformIcon from '@mui/icons-material/ChangeCircleOutlined';
import {
  default as BufferAccessIcon,
  default as BufferIcon,
} from '@mui/icons-material/CloudOutlined';
import ForkCloneIcon from '@mui/icons-material/ContentCopyOutlined';
import NodeIcon from '@mui/icons-material/ExtensionOutlined';
import SplitIcon from '@mui/icons-material/ForkLeftOutlined';
import ForkResult from '@mui/icons-material/ForkRightOutlined';
import ListenIcon from '@mui/icons-material/HearingOutlined';
import {
  default as JoinIcon,
  default as SerializedJoinIcon,
} from '@mui/icons-material/Merge';
import StreamOutIcon from '@mui/icons-material/Notes';
import ScopeIcon from '@mui/icons-material/Rectangle';
import SectionIcon from '@mui/icons-material/SelectAllOutlined';
import UnzipIcon from '@mui/icons-material/UnarchiveOutlined';
import type React from 'react';

import type { DiagramOperation } from '../types/diagram';
import { exhaustiveCheck } from '../utils/exhaustive-check';

export function getIcon(op: DiagramOperation): React.ComponentType {
  switch (op.type) {
    case 'node':
      return NodeIcon;
    case 'section':
      return SectionIcon;
    case 'fork_clone':
      return ForkCloneIcon;
    case 'unzip':
      return UnzipIcon;
    case 'fork_result':
      return ForkResult;
    case 'split':
      return SplitIcon;
    case 'join':
      return JoinIcon;
    case 'serialized_join':
      return SerializedJoinIcon;
    case 'transform':
      return TransformIcon;
    case 'buffer':
      return BufferIcon;
    case 'buffer_access':
      return BufferAccessIcon;
    case 'listen':
      return ListenIcon;
    case 'scope':
      return ScopeIcon;
    case 'stream_out':
      return StreamOutIcon;
    default:
      exhaustiveCheck(op);
      throw new Error('unknown op');
  }
}

export { default as TransformIcon } from '@mui/icons-material/ChangeCircleOutlined';
export {
  default as BufferAccessIcon,
  default as BufferIcon,
} from '@mui/icons-material/CloudOutlined';
export { default as ForkCloneIcon } from '@mui/icons-material/ContentCopyOutlined';
export { default as NodeIcon } from '@mui/icons-material/ExtensionOutlined';
export { default as SplitIcon } from '@mui/icons-material/ForkLeftOutlined';
export { default as ForkResultIcon } from '@mui/icons-material/ForkRightOutlined';
export { default as ListenIcon } from '@mui/icons-material/HearingOutlined';
export {
  default as JoinIcon,
  default as SerializedJoinIcon,
} from '@mui/icons-material/MergeOutlined';
export { default as StreamOutIcon } from '@mui/icons-material/Notes';
export { default as ScopeIcon } from '@mui/icons-material/Rectangle';
export { default as SectionIcon } from '@mui/icons-material/SelectAllOutlined';
export { default as UnzipIcon } from '@mui/icons-material/UnarchiveOutlined';
