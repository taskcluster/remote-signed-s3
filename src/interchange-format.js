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

async function validate(obj) {
  return new Promise((resolve, reject) => {
    Joi.validate(obj, InterchangeFormatSchema, (err, value) => {
      if (err) {
        reject(err);
      } else {
        resolve(err);
      }
    });
  });
}

async function validateList(list) {
  return await Promise.all(list.map(obj => validate(obj)));
}

module.exports = {
  validate,
  validateList,
  InterchangeFormatSchema,
}
