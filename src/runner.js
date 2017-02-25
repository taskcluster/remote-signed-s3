import crypto from 'crypto';
import https from 'https';
import urllib from 'url';
import stream from 'stream';

import _debug from 'debug';
import Joi from 'joi';

import DigestStream from './digest-stream';
import InterchangeFormat from './interchange-format';

const debug = _debug('remote-s3:Runner');
const debugRequest = _debug('remote-s3:Runner:req');
const debugResponse = _debug('remote-s3:Runner:res');

/**
 * This class runs General HTTP Requests.  It understands
 * objects which are like {url: '...', method: 'GET', headers: {}}
 * and return the body.  It will throw an exception for non-200
 * series responses by default.  The request should be run
 * with the `run()` method, which will return an object in the format
 * { body, headers, statusCode, statusMessage, requestHash, requestSize,
 *   responseHash, responseSize}.
 *
 * Note that the request and reponse size and hashes are for the body of each.
 * These values are primarily used to be able to double check that the data
 * actually transmitted matched a pre-computed size and hash
 */
class Runner {
  constructor(agent, agentOpts) {
    this.agent = agent || new https.Agent(agentOpts || {}); 
  }

  /**
   * Run a request exactly one time.  This means that we should
   * not do any retries.  This method takes a requst which
   * is in the standard interchange format of {url, method, header},
   * a body paramter.  The body parameter can be one of the following:
   *   1. 'string' primative -- written in a single .write(); call
   *   2. 'Buffer' object -- written in a single .write(); call
   *   3. 'Readable' object -- writen as a stream
   *   4. `() => Readable` function -- called without args to obtain
   *      a Readable
   */
  async run(opts) {
    let {req, body, streamingOutput} = opts;

    await InterchangeFormat.validate(req);

    let {url, method, headers} = req;

    method = method.toUpperCase();

    body = body || '';

    // Some sanity checking done
    if (body) {
      if (method === 'GET' || method === 'HEAD') {
        throw new Error('It is a violation of HTTP for GET or HEAD to have body');
      }

      if (typeof body !== 'string'
          && typeof body.pipe !== 'function'
          && typeof body !== 'function'
          && ! body instanceof Buffer) {
        throw new Error('If provided, body must be string, Readable, Buffer or function');
      }
    }

    // Wrap the request in a Promise
    return new Promise((resolve, reject) => {
      // We need to parse the URL for the basis of our request options
      // for the actual HTTP request
      let responseHash = crypto.createHash('sha256');
      let responseSize = 0;
      let requestHash;
      let requestSize;

      let parts = urllib.parse(url);
      parts.method = method;
      parts.headers = headers;
      let request = https.request(parts);

      function showRequest(err) {
        let str = `${method} ${url}`;
        if (Object.keys(headers).length > 0) {
          str += ` ${JSON.stringify(headers)}`;
        }
        if (requestHash && requestSize > 0) {
          str += ` ${requestHash} ${requestSize} bytes`;
        }
        if(err) {
          str += err;
        }
        debugRequest(str);
      };

      request.on('error', err => {
        showRequest(err);
        reject(err);
      });

      request.on('finish', () => {
        showRequest();
      });

      request.on('response', response => {
        let statusCode = response.statusCode;
        let statusMessage = response.statusMessage;
        let responseHeaders = response.headers;
        let responseChunks = [];

        function showResponse(err) {
          let str = `${statusCode} ${statusMessage} ${url}`;
          if (Object.keys(responseHeaders).length > 0) {
            str += ` ${JSON.stringify(responseHeaders)}`;
          }
          if (responseHash && responseSize > 0) {
            str += ` ${responseHash} ${responseSize} bytes`;
          }
          if(err) {
            str += err;
          }
          debugResponse(str);
        }

        if (streamingOutput) {
          // In the streaming case, we're going to still want to
          // log the information about the hash and size and outcome
          // of the response instead of just silently dropping it.
          //
          // That's why we're going to pipe the response through
          // a DigestStream to calculate the size and hash of the
          // response.  This can't be part of the resolution value
          // of the run() promise, since we'll resolve before we
          // know that.
          let digestStream = new DigestStream();
          
          // We want errors from the https.IncomingMessage instance
          // to be propogated to streams downstream
          response.on('error', err => {
            responseHash = digestStream.hash;
            responseSize = digestStream.size;
            showResponse(err);
            digestStream.emit('error', err);
          });

          // We want to be able to handle the other events of the
          // response
          response.on('aborted', () => { digestStream.emit('aborted') });
          response.on('close', () => { digestStream.emit('close') });
          
          digestStream.on('end', () => {
            responseHash = digestStream.hash;
            responseSize = digestStream.size;
            showResponse();
          });

          let output = {
            bodyStream: response.pipe(digestStream),
            headers: responseHeaders,
            statusCode,
            statusMessage,
            requestHash,
            requestSize,
          };
          Runner.validateOutput(output).then(resolve, reject);
        } else {
          response.on('error', err => {
            responseHash = responseHash.digest('hex');
            showResponse(err);
            reject(err);
          });

          // Maybe pipe the request to a DigestStream?
          response.on('data', data => {
            try {
              responseHash.update(data);
              responseSize += data.length;
              responseChunks.push(data);
            } catch (err) {
              reject(err);
            }
          });

          response.on('end', () => {
            try {
              responseHash = responseHash.digest('hex');
              
              showResponse();

              let responseBody = Buffer.concat(responseChunks);

              let output = {
                body: responseBody,
                headers: responseHeaders,
                statusCode,
                statusMessage,
                requestHash,
                requestSize,
                responseHash,
                responseSize
              };
              Runner.validateOutput(output).then(resolve, reject);
            } catch (err) {
              reject(err);
            }
          });
        }
      });

      if (typeof body === 'string' || body instanceof Buffer) {
        // Strings and Buffers are in the category of things which we just send
        // and then forget.  We write them in a single chunk since they're
        // already in memory as that data.
        requestHash = crypto
          .createHash('sha256')
          .update(body)
          .digest('hex');

        requestSize = body.length;

        request.write(body);
        request.end();
      } else if (typeof body.pipe === 'function' || typeof body === 'function') {
        // Readables and functions are in the category of things which we will
        // stream to the request.  In this case, we use Readables directly and
        // call the function synchronusly to obtain a Readable.
        let bodyStream;
        if (typeof body === 'function') {
          bodyStream = body();
        } else {
          bodyStream = body;
        }

        // We want to find out the hash and size of the request so that we can
        // log it for diagnostics.  This could also be used to ensure that the
        // bytes read from the disk actually match those we sent over the wire
        let digestStream = new DigestStream();

        request.on('error', err => {
          requestHash = digestStream.hash;
          requestSize = digestStream.size;
          reject(err);
        });

        request.on('aborted', () => {
          request.emit('error', new Error('Server Hangup'));
        });

        digestStream.on('end', () => {
          requestHash = digestStream.hash;
          requestSize = digestStream.size;
          request.end();
        });

        bodyStream.pipe(digestStream).pipe(request);
      } 
    });
  }
}

Runner.returnSchema = Joi.object().keys({
  body: Joi.object().type(Buffer),
  bodyStream: Joi.object().type(stream.Readable),
  headers: Joi.object().required(),
  statusCode: Joi.number().integer().min(100).max(599).required(),
  statusMessage: Joi.string().required(),
  requestHash: Joi.string().regex(/^[a-fA-F0-9]{64}$/).required(),
  requestSize: Joi.number().integer().min(0).required(),
  responseHash: Joi.string().regex(/^[a-fA-F0-9]{64}$/),
  responseSize: Joi.number().integer().min(0),
})
  .without('bodyStream', ['responseHash', 'responseSize', 'body'])
  .without('body', ['bodyStream'])
  .with('body', ['responseHash', 'responseSize']);

Runner.validateOutput = async function(output) {
  return new Promise((resolve, reject) => {
    Joi.validate(output, Runner.returnSchema, (err, value) => {
      if (err) {
        reject(err);
      } else {
        resolve(value);
      }
    });
  });
};

module.exports = Runner;
