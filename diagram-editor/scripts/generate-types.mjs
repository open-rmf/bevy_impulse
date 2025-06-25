import { execSync } from 'node:child_process';
import crypto from 'node:crypto';
import fs, { writeFileSync } from 'node:fs';
import { compile } from 'json-schema-to-typescript';

const schemaRaw = fs.readFileSync('../diagram.schema.json');
const hash = crypto.createHash('sha1').update(schemaRaw).digest('hex');
const schema = JSON.parse(schemaRaw);

// preprocess the schema to workaround https://github.com/bcherny/json-schema-to-typescript/issues/637 and https://github.com/bcherny/json-schema-to-typescript/issues/613
const workingSet = [...Object.values(schema.$defs)];
while (workingSet.length > 0) {
  const schema = workingSet.pop();
  if (typeof schema !== 'object') {
    continue;
  }

  if ('properties' in schema) {
    for (const property of Object.values(schema.properties)) {
      workingSet.push(property);
      // if (
      //   typeof property === 'object' &&
      //   '$ref' in property &&
      //   Object.keys(property).length > 1
      // ) {
      //   const $ref = property.$ref;
      //   for (const k of Object.keys(property)) {
      //     delete property[k];
      //   }
      //   property.$ref = $ref;
      // }
    }
  }

  if ('oneOf' in schema) {
    workingSet.push(...Object.values(schema.oneOf));
  }

  if ('anyOf' in schema) {
    workingSet.push(...Object.values(schema.anyOf));
  }

  // json schema draft-07 (used by json-schema-to-typescript) does not merge $ref, workaround
  // by putting the $ref and other fields in a `allOf`.
  if ('$ref' in schema && Object.keys(schema).length > 1) {
    const $ref = schema.$ref;
    const copy = {
      ...schema,
    };
    delete copy.$ref;

    for (const k of Object.keys(schema)) {
      delete schema[k];
    }

    if (Object.keys(copy).some((k) => !['description', 'title'].includes(k))) {
      // At least one field is not metadata only field.
      const allOf = [copy, { $ref }];
      schema.allOf = allOf;
    } else {
      // All the remaining fields are metadata only fields. We cannot use `allOf` as the
      // metadata only branch will allow any types.
      schema.$ref = $ref;
    }
  }
}

const output = await compile(schema, 'Diagram');
const fd = fs.openSync('frontend/types/diagram.d.ts', 'w');
fs.writeSync(fd, `// Generated from diagram.schema.json (sha1:${hash})\n`);
fs.writeSync(fd, output);
execSync('biome format --write ./frontend/types/diagram.d.ts');
writeFileSync(
  'frontend/diagram.preprocessed.schema.json',
  JSON.stringify(schema, undefined, 2),
);
