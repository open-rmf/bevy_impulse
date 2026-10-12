import { fireEvent, screen } from '@testing-library/react';
import { useState } from 'react';
import { createOperationNode } from '../nodes';
import BufferNode from '../nodes/buffer-node';
import { createOperationNodeProps, render } from '../nodes/test-utils';
import BufferForm from './buffer-form';

function BufferEditor() {
  const [node, setNode] = useState(() =>
    createOperationNode(
      'root',
      undefined,
      { x: 0, y: 0 },
      { type: 'buffer' },
      'temperature',
    ),
  );
  return (
    <>
      <BufferForm
        node={node}
        onChange={(change) => {
          if (change.type === 'replace') setNode(change.item as typeof node);
        }}
      />
      <BufferNode {...createOperationNodeProps(node)} />
    </>
  );
}

test('buffer labels can be edited and cleared without renaming the operation', () => {
  render(<BufferEditor />);
  const label = screen.getByRole('textbox', { name: 'Display Text' });
  fireEvent.change(label, { target: { value: 'Latest reading' } });
  expect(screen.getByTitle('Latest reading')).toBeVisible();
  expect(screen.getByRole('textbox', { name: /^id/ })).toHaveValue(
    'temperature',
  );

  fireEvent.change(label, { target: { value: '' } });
  expect(screen.getByTitle('temperature')).toBeVisible();
  expect(screen.getByRole('textbox', { name: /^id/ })).toHaveValue(
    'temperature',
  );
});
