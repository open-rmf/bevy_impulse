import { render } from '@testing-library/react';
import type { AddOperationKey } from '../utils/add-operation-catalog';
import { getAddOperationIcon, OPERATION_ICONS } from './icons';

describe('getAddOperationIcon', () => {
  const allOperationKeys: AddOperationKey[] = [
    'sectionInput',
    'sectionOutput',
    'sectionBuffer',
    'node',
    'fork_clone',
    'unzip',
    'fork_result',
    'split',
    'join',
    'transform',
    'buffer',
    'buffer_access',
    'listen',
    'stream_out',
    'scope',
    'section',
    'script',
  ];

  test('contains icon definitions for all AddOperationKey values', () => {
    for (const key of allOperationKeys) {
      expect(OPERATION_ICONS[key]).toBeDefined();
    }
  });

  test('allOperationKeys matches OPERATION_ICONS keys exactly', () => {
    expect(Object.keys(OPERATION_ICONS).sort()).toEqual(
      [...allOperationKeys].sort(),
    );
  });

  test.each(allOperationKeys)(
    'returns defined icon element for built-in operation %s',
    (key) => {
      const iconElement = getAddOperationIcon(key);
      expect(iconElement).toBeDefined();

      const { container } = render(<div>{iconElement}</div>);
      const symbolSpan = container.querySelector('.material-symbols-outlined');
      expect(symbolSpan).toBeInTheDocument();
      expect(symbolSpan?.textContent).toBeTruthy();
    },
  );

  test('maps specific operations to their expected material symbol names', () => {
    const expectations: Record<string, string> = {
      transform: 'change_circle',
      fork_clone: 'content_copy',
      unzip: 'format_list_numbered',
      fork_result: 'question_mark',
      split: 'call_split',
      join: 'call_merge',
      buffer: 'database',
      buffer_access: 'database_upload',
      listen: 'hearing',
      stream_out: 'notes',
      scope: 'rectangle',
      section: 'select_all',
      script: 'code',
      sectionInput: 'input',
      sectionOutput: 'output',
      sectionBuffer: 'database',
      node: 'line_start_circle',
    };

    for (const [key, expectedSymbol] of Object.entries(expectations)) {
      const { container } = render(
        <div data-testid={`icon-${key}`}>
          {getAddOperationIcon(key as AddOperationKey)}
        </div>,
      );
      const symbolSpan = container.querySelector('.material-symbols-outlined');
      expect(symbolSpan?.textContent).toBe(expectedSymbol);
    }
  });

  test('returns NodeIcon (line_start_circle) for registry node builders with node: prefix', () => {
    const { container } = render(
      <div>{getAddOperationIcon('node:custom_calculator')}</div>,
    );
    const symbolSpan = container.querySelector('.material-symbols-outlined');
    expect(symbolSpan?.textContent).toBe('line_start_circle');
  });

  test('falls back gracefully to NodeIcon for unknown keys, prototype methods, and non-string inputs', () => {
    const dangerousKeys = [
      'unknown_key',
      'toString',
      'valueOf',
      'constructor',
      'hasOwnProperty',
      '__proto__',
      undefined,
      null,
      123,
    ];

    for (const key of dangerousKeys) {
      const { container } = render(
        <div>
          {getAddOperationIcon(
            key as unknown as Parameters<typeof getAddOperationIcon>[0],
          )}
        </div>,
      );
      const symbolSpan = container.querySelector('.material-symbols-outlined');
      expect(symbolSpan?.textContent).toBe('line_start_circle');
    }
  });
});
