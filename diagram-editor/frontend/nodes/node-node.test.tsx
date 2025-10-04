import { HandleId } from '../handles';
import type { DiagramElementRegistry } from '../types/api';
import { ROOT_NAMESPACE } from '../utils/namespace';
import { createOperationNode } from './create-node';
import NodeNode from './node-node';
import { createOperationNodeProps, render } from './test-utils';

describe('NodeNode', () => {
  test('render successfully', () => {
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

  test('contain only data handles if the builder does not have streams', () => {
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
    // check for stream handle
    expect(
      fixture.baseElement.querySelector(
        `.react-flow__handle[data-handleid=${HandleId.DataStream}]`,
      ),
    ).toBeFalsy();
  });

  test('contain data and stream handles if the builder has streams', () => {
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
    const registry: DiagramElementRegistry = {
      messages: {},
      nodes: {
        test_builder: {
          config_schema: true,
          default_display_text: 'test',
          request: 'request',
          response: 'response',
          streams: {
            test_stream: 'stream_type',
          },
        },
      },
      schemas: {},
      sections: {},
      trace_supported: false,
    };
    const fixture = render(<NodeNode {...nodeProps} />, registry);

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
