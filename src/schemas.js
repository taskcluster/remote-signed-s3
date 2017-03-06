import crypto from 'crypto';

import Joi from 'joi';

const KB = 1024;
const MB = KB * 1024;
const GB = MB * 1024;
const TB = GB * 1024;

// This the list of canned ACLs that are valid.
const cannedACLs = [
  'private',
  'public-read',
  'public-read-write',
  'aws-exec-read',
  'authenticated-read',
  'bucket-owner-read',
  'bucket-owner-full-control',
];

// This is the list of valid storage classes
const storageClasses = [
  'STANDARD',
  'STANDARD_IA',
  'REDUCED_REDUNDANCY',
];

/**
 * Joi schema to represent Permissions objects
 *
 * http://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
 */
const schemas = {
  permissions: Joi.object().keys({
    acl: Joi.string().valid(cannedACLs),
    read: Joi.string(),
    write: Joi.string(),
    readAcp: Joi.string(),
    writeAcp: Joi.string(),
    fullControl: Joi.string(),
  }).without('acl', ['read', 'write', 'readAcp', 'writeAcp', 'fullControl']),
  bucket: Joi.string().regex(/^[a-z0-9][a-z0-9-]{2,62}$/),
  key: Joi.string().min(1).max(1024),
  // Not sure if we should have these restrictions.  I'm saying here that we
  // cannot upload zero-length files and that we cannot load files or parts
  // which match the empty string.  I can see there being a case for an empty
  // file, but I suspect that the better approach would be to store something
  // in Azure in the Entity for *tiny* things, especially empty files
  sha256: Joi.string().hex().length(64)
    .regex(new RegExp(`^${crypto.createHash('sha256').update('').digest('hex')}`), {invert:true}),
  spSize: Joi.number().min(1).max(5 * GB),
  mpSize: Joi.number().min(1).max(5 * TB),
  uploadId: Joi.string(),
  etags: Joi.array().min(1).max(10000).items(Joi.string()),
  tags: Joi.object(),
  metadata: Joi.object(),
  contentType: Joi.string().default('binary/octet-stream'),
  contentDisposition: Joi.string(),
  contentEncoding: Joi.string(),
  storageClass: Joi.string().valid(storageClasses).default('STANDARD'),
};

// These are the schemas which reference other schemas
schemas.parts = Joi.array().min(1).max(10000).items(Joi.object().keys({
  sha256: schemas.sha256,
  size: Joi.number().min(0).max(5 * 1024 * 1024 * 1024),
  start: Joi.number().min(0).max(5 * TB - 5 * GB),
}));

/**
 * Take a Joi schema and an object to validate it against.
 * If the object has a validation error, it will be thrown and the value
 * will be returned with any possible defaults being substituted in
 */
function runSchema(obj, schema) {
  let result = schema.validate(obj);
  if (result.error) {
    throw result.error;
  }
  return result.value;
}

module.exports = {
  Joi,
  schemas,
  runSchema,
  KB, MB, GB, TB,
};
