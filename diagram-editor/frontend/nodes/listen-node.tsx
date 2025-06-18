import type { NodeProps } from '@xyflow/react';
import BaseNode from './base-node';
import { ListenIcon } from './icons';
import type { DiagramEditorNode } from './types';

function ListenNode(props: NodeProps<DiagramEditorNode>) {
  return (
    <BaseNode
      {...props}
      icon={<ListenIcon />}
      label="Listen"
      variant="inputOutput"
    />
  );
}

export default ListenNode;
