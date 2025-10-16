import { Box, type BoxProps } from '@mui/material';
import type React from 'react';
import type { DiagramOperation } from '../types/api';
import { exhaustiveCheck } from '../utils/exhaustive-check';

export interface MaterialSymbolProps extends BoxProps {
  symbol: string;
}

export function MaterialSymbol({
  symbol,
  className,
  ...otherProps
}: MaterialSymbolProps): React.JSX.Element {
  return (
    <Box
      component="span"
      className={`material-symbols-outlined ${className}`}
      {...otherProps}
    >
      {symbol}
    </Box>
  );
}

export function NodeIcon(): React.JSX.Element {
  return <MaterialSymbol symbol="line_start_circle" />;
}

export function ForkCloneIcon(): React.JSX.Element {
  return <MaterialSymbol symbol="content_copy" />;
}

export function TransformIcon(): React.JSX.Element {
  return <MaterialSymbol symbol="change_circle" />;
}

export function BufferIcon(): React.JSX.Element {
  return <MaterialSymbol symbol="database" />;
}

export function BufferAccessIcon(): React.JSX.Element {
  return <MaterialSymbol symbol="database_upload" />;
}

export function SplitIcon(): React.JSX.Element {
  return (
    <MaterialSymbol symbol="call_split" sx={{ transform: 'scaleY(-1)' }} />
  );
}

export function ForkResultIcon(): React.JSX.Element {
  return <MaterialSymbol symbol="question_mark" />;
}

export function ListenIcon(): React.JSX.Element {
  return <MaterialSymbol symbol="hearing" />;
}

export function JoinIcon(): React.JSX.Element {
  return <MaterialSymbol symbol="arrow_and_edge" />;
}

export function StreamOutIcon(): React.JSX.Element {
  return <MaterialSymbol symbol="notes" />;
}

export function ScopeIcon(): React.JSX.Element {
  return <MaterialSymbol symbol="rectangle" />;
}

export function SectionIcon(): React.JSX.Element {
  return <MaterialSymbol symbol="select_all" />;
}

export function SectionInputIcon(): React.JSX.Element {
  return <MaterialSymbol symbol="input" />;
}

export function SectionOutputIcon(): React.JSX.Element {
  return <MaterialSymbol symbol="output" />;
}

export function SectionBufferIcon(): React.JSX.Element {
  return <MaterialSymbol symbol="database" />;
}

export function UnzipIcon(): React.JSX.Element {
  return <MaterialSymbol symbol="format_list_numbered" />;
}

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
      return ForkResultIcon;
    case 'split':
      return SplitIcon;
    case 'join':
      return JoinIcon;
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
