import crypto from 'node:crypto';
import fs from 'node:fs';
import { execSync } from 'node:child_process';
import { compile } from 'json-schema-to-typescript';

const schema = fs.readFileSync('../diagram.schema.json');
const hash = crypto.createHash('sha1').update(schema).digest('hex');
const output = await compile(JSON.parse(schema));
const fd = fs.openSync('src/types/diagram.d.ts', 'w');
fs.writeSync(fd, `// Generated from diagram.schema.json (sha1:${hash})\n`);
fs.writeSync(fd, output);
execSync('biome format --write ./src/types/diagram.d.ts');
