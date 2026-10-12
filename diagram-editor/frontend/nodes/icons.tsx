import { Box, type BoxProps } from '@mui/material';
import type React from 'react';
import type {
  AddOperationCandidateKey,
  AddOperationKey,
} from '../utils/add-operation-catalog';

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

export function ForkResultIcon(): React.JSX.Element {
  return <MaterialSymbol symbol="question_mark" />;
}

export function TransformIcon(): React.JSX.Element {
  return <MaterialSymbol symbol="change_circle" />;
}

export function ScriptIcon(): React.JSX.Element {
  return <MaterialSymbol symbol="code" />;
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

export function ListenIcon(): React.JSX.Element {
  return <MaterialSymbol symbol="hearing" />;
}

export function JoinIcon(): React.JSX.Element {
  return (
    <MaterialSymbol symbol="call_merge" sx={{ transform: 'scaleY(-1)' }} />
  );
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

export const OPERATION_ICONS: Record<AddOperationKey, React.ReactNode> = {
  sectionInput: <SectionInputIcon />,
  sectionOutput: <SectionOutputIcon />,
  sectionBuffer: <SectionBufferIcon />,
  node: <NodeIcon />,
  fork_clone: <ForkCloneIcon />,
  unzip: <UnzipIcon />,
  fork_result: <ForkResultIcon />,
  split: <SplitIcon />,
  join: <JoinIcon />,
  transform: <TransformIcon />,
  buffer: <BufferIcon />,
  buffer_access: <BufferAccessIcon />,
  listen: <ListenIcon />,
  stream_out: <StreamOutIcon />,
  scope: <ScopeIcon />,
  section: <SectionIcon />,
  script: <ScriptIcon />,
};

const VALID_OPERATION_KEYS = new Set<string>(Object.keys(OPERATION_ICONS));

export function getAddOperationIcon(
  key: AddOperationCandidateKey | string,
): React.ReactNode {
  if (typeof key === 'string' && key.startsWith('node:')) {
    return OPERATION_ICONS.node;
  }
  if (typeof key === 'string' && VALID_OPERATION_KEYS.has(key)) {
    return OPERATION_ICONS[key as AddOperationKey];
  }
  return OPERATION_ICONS.node;
}
