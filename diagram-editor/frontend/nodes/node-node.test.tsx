import { HandleId } from '../handles';
import { ROOT_NAMESPACE } from '../utils/namespace';
import { createOperationNode } from './create-node';
import NodeNode from './node-node';
import { createOperationNodeProps, render } from './test-utils.test';

describe('NodeNode', () => {
  it('should render successfully', () => {
    const nodeNode = createOperationNode<'node'>(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      {
        type: 'node',
        builder: 'test_builder',
        next: 'test_next_operation',
      },
      'test_op_id',
    );
    const nodeProps = createOperationNodeProps(nodeNode);
    const { baseElement } = render(<NodeNode {...nodeProps} />);
    expect(baseElement).toBeTruthy();
  });

  it('should contain data and stream handles', () => {
    const nodeNode = createOperationNode<'node'>(
      ROOT_NAMESPACE,
      undefined,
      { x: 0, y: 0 },
      {
        type: 'node',
        builder: 'test_builder',
        next: 'test_next_operation',
      },
      'test_op_id',
    );
    const nodeProps = createOperationNodeProps(nodeNode);
    const fixture = render(<NodeNode {...nodeProps} />);

    // check for data target handle
    expect(
      fixture.baseElement.querySelector(
        '.react-flow__handle.target:not([data-handleid])',
      ),
    ).toBeTruthy();
    // check for data source handle
    expect(
      fixture.baseElement.querySelector(
        '.react-flow__handle.source:not([data-handleid])',
      ),
    ).toBeTruthy();
    // check for stream source handle
    expect(
      fixture.baseElement.querySelector(
        `.react-flow__handle.source[data-handleid=${HandleId.DataStream}]`,
      ),
    ).toBeTruthy();
  });
});
