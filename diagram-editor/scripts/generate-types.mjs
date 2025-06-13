import { execSync } from 'node:child_process';
import crypto from 'node:crypto';
import fs from 'node:fs';
import { compile } from 'json-schema-to-typescript';

const schemaRaw = fs.readFileSync('../diagram.schema.json');
const hash = crypto.createHash('sha1').update(schemaRaw).digest('hex');
const schema = JSON.parse(schemaRaw);

// normalize the schema to workaround https://github.com/bcherny/json-schema-to-typescript/issues/637.
const toCheck = Object.values(schema.definitions);
while (toCheck.length) {
  const def = toCheck.pop();
  toCheck.push(...(def.allOf || []));
  toCheck.push(...(def.anyOf || []));
  toCheck.push(...(def.oneOf || []));

  if ('oneOf' in def && 'properties' in def) {
    // move properties defined in the same schema level as oneOf into an allOf
    const directProps = { properties: def.properties };
    if ('required' in def) {
      directProps.required = def.required;
    }
    def.allOf = [{ oneOf: def.oneOf }, directProps];
    // biome-ignore lint/performance/noDelete:
    delete def.properties;
    // biome-ignore lint/performance/noDelete:
    delete def.required;
    // biome-ignore lint/performance/noDelete:
    delete def.oneOf;
  }
}

const output = await compile(schema);
const fd = fs.openSync('src/types/diagram.d.ts', 'w');
fs.writeSync(fd, `// Generated from diagram.schema.json (sha1:${hash})\n`);
fs.writeSync(fd, output);
execSync('biome format --write ./src/types/diagram.d.ts');
