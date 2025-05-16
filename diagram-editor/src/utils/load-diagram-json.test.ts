import { test, expect } from 'vitest';
import { loadDiagramJson } from './load-diagram-json';
import testDiagram from './test-data/multiply3.json';

test('load diagram json', () => {
  const nodes = loadDiagramJson(JSON.stringify(testDiagram));
  expect(nodes.length).toBe(1);
});
