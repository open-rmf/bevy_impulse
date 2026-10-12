import { styled } from '@mui/material';
import { type NodeProps, Position } from '@xyflow/react';
import { Handle, HandleId, HandleType } from '../handles';
import type { OperationNode } from '.';
import BaseNode from './base-node';
import { ForkResultIcon } from './icons';

const ForkResultOkHandle = styled(Handle)(({ theme }) => ({
  left: '25%',
  backgroundColor: theme.palette.success.main,
}));
const ForkResultErrHandle = styled(Handle)(({ theme }) => ({
  left: '75%',
  backgroundColor: theme.palette.error.main,
}));

function ForkResultNodeComp(props: NodeProps<OperationNode<'fork_result'>>) {
  return (
    <BaseNode
      {...props}
      compact
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
