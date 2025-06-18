import { execSync } from 'node:child_process';
import crypto from 'node:crypto';
import fs, { copyFileSync } from 'node:fs';
import { compile } from 'json-schema-to-typescript';

const schemaRaw = fs.readFileSync('../diagram.schema.json');
const hash = crypto.createHash('sha1').update(schemaRaw).digest('hex');
const schema = JSON.parse(schemaRaw);

function fixJsonSchema(schema) {
  const combinations = new Set(['oneOf', 'allOf', 'anyOf']);
  const inheritance = new Set([...combinations, '$ref']);

  for (const k of combinations) {
    if (k in schema) {
      for (const v of schema[k]) {
        fixJsonSchema(v);
      }
    }
  }

  if ('properties' in schema) {
    for (const propertyKey of Object.keys(schema.properties)) {
      const propertyValue = schema.properties[propertyKey];
      // having other fields and $ref causes json-schema-to-typescript to inline it, workaround
      // by removing all other fields.
      if (
        typeof propertyValue === 'object' &&
        Object.keys(propertyValue).length > 1 &&
        '$ref' in propertyValue
      ) {
        schema.properties[propertyKey] = { $ref: propertyValue.$ref };
      }
    }
  }

  const keys = Object.keys(schema);
  if (keys.length > 1 && keys.some((k) => inheritance.has(k))) {
    const allOf = [];
    const otherFields = {};
    for (const k of keys) {
      if (inheritance.has(k)) {
        allOf.push({ [k]: schema[k] });
      } else {
        otherFields[k] = schema[k];
      }
      delete schema[k];
    }
    allOf.push(otherFields);
    schema.allOf = allOf;
  }
}

// preprocess the schema to workaround https://github.com/bcherny/json-schema-to-typescript/issues/637 and https://github.com/bcherny/json-schema-to-typescript/issues/613
for (const def of Object.values(schema.$defs)) {
  fixJsonSchema(def);
}

const output = await compile(schema, 'Diagram', {});
const fd = fs.openSync('frontend/types/diagram.d.ts', 'w');
fs.writeSync(fd, `// Generated from diagram.schema.json (sha1:${hash})\n`);
fs.writeSync(fd, output);
execSync('biome format --write ./frontend/types/diagram.d.ts');
copyFileSync('../diagram.schema.json', './frontend/diagram.schema.json');
