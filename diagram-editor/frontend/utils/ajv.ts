import Ajv from 'ajv/dist/2020';
import type { AnyValidateFunction } from 'ajv/dist/core';
import addFormats from 'ajv-formats';
import apiSchema from '../api.preprocessed.schema.json';

const ajv = addFormats(new Ajv({ allowUnionTypes: true })).addFormat(
  'uint',
  /^[0-9]+$/,
);
ajv.compile(apiSchema);

export function getSchema<T>(
  key: keyof (typeof apiSchema)['$defs'],
): AnyValidateFunction<T> {
  const validate = ajv.getSchema<T>(`#/$defs/${key}`);
  if (!validate) {
    throw new Error(`cannot validate ${key}`);
  }
  return validate;
}

export default ajv;
