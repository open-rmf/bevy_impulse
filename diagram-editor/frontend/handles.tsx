import {
  Handle as ReactFlowHandle,
  type HandleProps as ReactFlowHandleProps,
  useConnection,
  useNodeId,
} from '@xyflow/react';
import { useDraggedConnectionCompatibility } from './connection-compatibility-provider';
import { exhaustiveCheck } from './utils/exhaustive-check';

export enum HandleId {
  DataStream = 'dataStream',
  ForkResultOk = 'forkResultOk',
  ForkResultErr = 'forkResultErr',
}

export enum HandleType {
  Data,
  Buffer,
  DataStream,
  DataBuffer,
}

export interface HandleProps extends Omit<ReactFlowHandleProps, 'id'> {
  /**
   * Id of the handle, this affects how the validator determines if a connection is valid.
   * For variants other than `HandleType.Data`, you probably want to assign an appropriate id.
   */
  id?: HandleId;
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

export function Handle({ id, variant, className, ...baseProps }: HandleProps) {
  const nodeId = useNodeId();
  const connection = useConnection();
  const handleType = baseProps.type || 'source';
  const compatibility = useDraggedConnectionCompatibility({
    otherNodeId: nodeId,
    otherHandleId: id,
    otherHandleType: handleType,
  });

  const classNames: string[] = [];
  const variantClass = variantClassName(variant);
  if (variantClass) {
    classNames.push(variantClass);
  }
  if (className) {
    classNames.push(className);
  }

  if (
    compatibility &&
    compatibility.status !== 'unknown' &&
    connection.inProgress
  ) {
    classNames.push(
      compatibility.status === 'compatible'
        ? 'handle-compatible'
        : 'handle-incompatible',
    );
  }

  return (
    <ReactFlowHandle
      {...baseProps}
      id={id}
      className={classNames.length > 0 ? classNames.join(' ') : undefined}
    />
  );
}
