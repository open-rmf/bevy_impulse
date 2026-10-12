import { fireEvent, screen } from '@testing-library/react';
import { applyNodeChanges } from '@xyflow/react';
import { useState } from 'react';
import { DiagramPropertiesProvider } from '../diagram-properties-provider';
import { DiagramSidePanelProvider } from '../diagram-side-panel-controller';
import {
  createOperationNode,
  type DiagramEditorNode,
  isOperationNode,
  type OperationNode,
} from '../nodes';
import { render } from '../nodes/test-utils';
import { updateOperationIds } from '../utils/operation-id';
import NodeForm from './node-form';
import ScriptForm from './script-form';

function Editor({ script = false }: { script?: boolean }) {
  const [nodes, setNodes] = useState<DiagramEditorNode[]>(() => [
    createOperationNode(
      '',
      undefined,
      { x: 0, y: 0 },
      script
        ? {
            type: 'script',
            environment: '',
            run: 'execute',
            next: { builtin: 'dispose' },
          }
        : { type: 'node', builder: 'add', next: { builtin: 'dispose' } },
    ),
  ]);
  const node = nodes[0];
  const onChange = (
    change: Parameters<typeof updateOperationIds>[0][number],
  ) => {
    setNodes((previous) =>
      applyNodeChanges(updateOperationIds([change], previous), previous),
    );
  };
  return (
    <>
      <DiagramPropertiesProvider>
        <DiagramSidePanelProvider>
          {script ? (
            <ScriptForm
              node={node as OperationNode<'script'>}
              onChange={onChange}
            />
          ) : (
            <NodeForm
              node={node as OperationNode<'node'>}
              onChange={onChange}
            />
          )}
        </DiagramSidePanelProvider>
      </DiagramPropertiesProvider>
      <output>{isOperationNode(node) && node.data.opId}</output>
    </>
  );
}

test('manual IDs survive builder edits; clearing the field restores automatic naming', () => {
  render(<Editor />);
  const id = screen.getByRole('textbox', { name: /^id/ });
  const generatedId = screen.getByRole('status').textContent;
  const automaticColor = getComputedStyle(id).color;
  fireEvent.focus(id);
  expect(id).toHaveValue(generatedId);
  fireEvent.change(id, { target: { value: '' } });
  expect(id).toHaveValue('');
  expect(screen.getByRole('status').textContent).toBe(generatedId);
  fireEvent.blur(id);
  expect(id).toHaveValue(generatedId);
  expect(
    screen.queryByText('Leave blank to generate an ID automatically.'),
  ).not.toBeInTheDocument();
  fireEvent.change(id, { target: { value: 'my_addition' } });
  expect(getComputedStyle(id).color).not.toBe(automaticColor);
  const builder = screen.getByRole('combobox', { name: /Builder/ });
  fireEvent.change(builder, { target: { value: 'subtract' } });
  fireEvent.blur(builder);
  expect(screen.getByRole('status')).toHaveTextContent('my_addition');
  fireEvent.focus(id);
  fireEvent.change(id, { target: { value: '' } });
  expect(id).toHaveValue('');
  fireEvent.blur(id);
  expect(screen.getByRole('status').textContent).toMatch(
    /^node_subtract_[A-Z0-9]{6}$/,
  );
  expect(getComputedStyle(id).color).toBe(automaticColor);
  expect(id).toHaveValue(screen.getByRole('status').textContent);
  fireEvent.change(builder, { target: { value: 'multiply' } });
  fireEvent.blur(builder);
  expect(screen.getByRole('status').textContent).toMatch(
    /^node_multiply_[A-Z0-9]{6}$/,
  );
  expect(id).toHaveValue(screen.getByRole('status').textContent);
});

test('script IDs follow the entrypoint while manual IDs stay fixed', () => {
  render(<Editor script />);
  const id = screen.getByRole('textbox', { name: /^id/ });
  const entrypoint = screen.getByRole('combobox', { name: /Entrypoint/ });
  fireEvent.change(entrypoint, { target: { value: 'process' } });
  expect(screen.getByRole('status').textContent).toMatch(
    /^script_process_[A-Z0-9]{6}$/,
  );
  fireEvent.change(id, { target: { value: 'my_script' } });
  fireEvent.change(entrypoint, { target: { value: 'execute' } });
  expect(screen.getByRole('status')).toHaveTextContent('my_script');
});
