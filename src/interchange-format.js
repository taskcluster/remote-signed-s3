import Joi from 'joi';

// https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods
const httpMethods = [
  'GET', 'get',
  'HEAD', 'head',
  'PUT', 'put',
  'POST', 'post',
  'PATCH', 'patch',
  'DELETE', 'delete',
  'OPTIONS', 'options',
  'TRACE', 'trace',
  'CONNECT', 'connect',
];

// TODO: Figure out how to specify headers better
const InterchangeFormatSchema = Joi.object().keys({
  url: Joi.string().regex(/^https?:/).required(),
  method: Joi.valid(httpMethods).required(),
  headers: Joi.object().required(),
});

function validate(obj) {
  let result = Joi.validate(obj, InterchangeFormatSchema);
  if (result.error) {
    throw new Error(result.error);
  }
  return obj;
}

module.exports = {
  validate,
  InterchangeFormatSchema,
}
