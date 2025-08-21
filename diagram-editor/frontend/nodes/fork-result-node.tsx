import { styled } from '@mui/material';
import { type NodeProps, Position } from '@xyflow/react';
import { Handle, HandleId, HandleType } from '../handles';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { ForkResultIcon } from './icons';

const ForkResultOkHandle = styled(Handle)(({ theme }) => ({
  left: '25%',
  background: `linear-gradient(-45deg, ${theme.palette.success.main} 50%, var(--xy-handle-background-color-default) 50%)`,
}));
const ForkResultErrHandle = styled(Handle)(({ theme }) => ({
  left: '75%',
  background: `linear-gradient(-45deg, ${theme.palette.error.main} 50%, var(--xy-handle-background-color-default) 50%)`,
}));

function ForkResultNodeComp(props: NodeProps<OperationNode<'fork_result'>>) {
  return (
    <BaseNode
      {...props}
      icon={<ForkResultIcon />}
      label="Fork Result"
      handles={
        <>
          <Handle
            type="target"
            position={Position.Top}
            isConnectable={props.isConnectable}
            variant={HandleType.Data}
          />
          <ForkResultOkHandle
            id={HandleId.ForkResultOk}
            type="source"
            position={Position.Bottom}
            isConnectable={props.isConnectable}
            variant={HandleType.Data}
          />
          <ForkResultErrHandle
            id={HandleId.ForkResultErr}
            type="source"
            position={Position.Bottom}
            isConnectable={props.isConnectable}
            variant={HandleType.Data}
          />
        </>
      }
    />
  );
}

export default ForkResultNodeComp;
