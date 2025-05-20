import Ajv from 'ajv';
import addFormats from 'ajv-formats';
import diagramSchema from '../diagram.schema.json';
import type { DiagramEditorNode } from '../nodes';
import type { Diagram } from '../types/diagram';

const ajv = new Ajv();
addFormats(ajv);
ajv.addFormat('uint', /^[0-9]+$/);
const validate = ajv.compile<Diagram>(diagramSchema);

export function loadDiagramJson(jsonStr: string): DiagramEditorNode[] {
  const diagram = JSON.parse(jsonStr);
  const valid = validate(diagram);
  if (!valid) {
    throw validate.errors;
  }

  return Object.entries(diagram.ops).map(
    ([id, data]) =>
      ({
        id,
        position: { x: 0, y: 0 },
        data,
      }) satisfies DiagramEditorNode,
  );
}
