import Ajv from 'ajv/dist/2020';
import addFormats from 'ajv-formats';

const ajv = new Ajv({ allowUnionTypes: true });
addFormats(ajv);
ajv.addFormat('uint', /^[0-9]+$/);

export default ajv;
