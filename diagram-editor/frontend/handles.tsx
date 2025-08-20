import {
  Handle as ReactFlowHandle,
  type HandleProps as ReactFlowHandleProps,
} from '@xyflow/react';
import { exhaustiveCheck } from './utils/exhaustive-check';

export type HandleId = 'dataStream' | null | undefined;

export enum HandleType {
  Data,
  Buffer,
  DataStream,
  DataBuffer,
}

export interface HandleProps extends Omit<ReactFlowHandleProps, 'id'> {
  variant: HandleType;
}

function variantClassName(handleType?: HandleType): string | undefined {
  if (handleType === undefined) {
    return undefined;
  }

  switch (handleType) {
    case HandleType.Data: {
      // use the default style
      return undefined;
    }
    case HandleType.Buffer: {
      return 'handle-buffer';
    }
    case HandleType.DataBuffer: {
      return 'handle-data-buffer';
    }
    case HandleType.DataStream: {
      return 'handle-data-stream';
    }
    default: {
      exhaustiveCheck(handleType);
      throw new Error('unknown edge category');
    }
  }
}

export function Handle({ variant, className, ...baseProps }: HandleProps) {
  const handleId: HandleId =
    variant === HandleType.DataStream ? 'dataStream' : undefined;

  const prependClassName = className
    ? `${variantClassName(variant)} ${className} `
    : variantClassName(variant);

  return (
    <ReactFlowHandle
      {...baseProps}
      id={handleId}
      className={prependClassName}
    />
  );
}
