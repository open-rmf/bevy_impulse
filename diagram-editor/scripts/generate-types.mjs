import { execSync } from 'node:child_process';
import fs, { writeFileSync } from 'node:fs';
import { compile } from 'json-schema-to-typescript';

async function generate(name, schema, outputPath, preprocessedOutputPath) {
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

      if (
        Object.keys(copy).some((k) => !['description', 'title'].includes(k))
      ) {
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

  const output = await compile(schema, name, { unreachableDefinitions: true });
  const fd = fs.openSync(outputPath, 'w');
  fs.writeSync(fd, output);
  fs.closeSync(fd);

  writeFileSync(preprocessedOutputPath, JSON.stringify(schema, undefined, 2));
}

const apiSchema = execSync(
  'cargo run -p bevy_impulse_diagram_editor -F json_schema --bin print_schema',
  {
    encoding: 'utf-8',
    stdio: 'pipe',
  },
);

await generate(
  'DiagramEditorApi',
  JSON.parse(apiSchema),
  'frontend/types/api.d.ts',
  'frontend/api.preprocessed.schema.json',
);

execSync('biome format --write frontend/types/api.d.ts');
