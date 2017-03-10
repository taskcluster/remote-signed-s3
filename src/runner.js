import crypto from 'crypto';
import http from 'http';
import https from 'https';
import urllib from 'url';
import stream from 'stream';

import _debug from 'debug';
import {Joi, schemas, runSchema} from './schemas';

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
  constructor(opts) {
    opts = runSchema(opts || {}, Joi.object().keys({
      agent: Joi.object(),
      agentOpts: Joi.object(),
      maxRetries: Joi.number().min(0).max(10).default(5),
      retryDelayFactor: Joi.number().min(100).default(100),
      retryDelayJitter: Joi.number().min(0).default(0),
    }).without('agent', 'agentOpts').optionalKeys(['agent', 'agentOpts']));

    this.agent = opts.agent || new https.Agent(opts.agentOpts || {}); 

    this.maxRetries = opts.maxRetries;
    this.retryDelayFactor = opts.retryDelayFactor;
    this.retryDelayJitter = opts.retryDelayJitter;

  }

  /**
   * Run a request and retry if a retryable error occurs.  Only 500-series
   * responses are considered retriable, so any other status code will return
   * with its status.
   *
   * http://docs.aws.amazon.com/general/latest/gr/api-retries.html
   */
  async run(opts) {
    let {req, body, streamingOutput, followRedirect, currentRedirect, maxRedirects} = opts;
    let current = 0;

    currentRedirect = currentRedirect || 0;
    maxRedirects = maxRedirects || 10;

    // Since streams aren't seekable in node, we want to ensure that
    // whenever 
    if (body && body instanceof stream.Readable || body && typeof body.pipe === 'function') {
      debug('WARNING: instances of stream.Readable are not retryable or redirectable');
      current = this.maxRetries - 1;
      followRedirect = false;
    }

    let sleepFor = (n) => {
      return new Promise((resolve, reject) => {
        let delay = Math.pow(2, n) * this.retryDelayFactor;
        let rf = this.retryDelayJitter;
        delay = delay * (Math.random() * 2 * rf + 1 - rf);
        setTimeout(resolve, delay);
      });
    }

    // Note that we've declared current earlier so that we could ensure that
    // raw streams do not allow for retry
    for ( ; current < this.maxRetries; current++) {
      let result = await this.runOnce(opts);
      if (result.statusCode >= 500 && current !== this.maxRetries - 1) {
        let errorSummary = `${result.statusCode || '???'}: ${result.statusMessage || '???'}`;
        debug(`RETRYABLE ERROR ${req.method} ${req.url} --> ${errorSummary}`);
        await sleepFor(current);
        continue;
      } else if (followRedirect) {
        // https://www.ietf.org/rfc/rfc2616.txt -- 10.3 Redirection 3xx
        // These are the only 300 series codes that we're sure we can safely
        // redirect.  Basically, anything which is not a known to be safe
        // redirect will be returned as the 300 series response rather than
        // following them.  If a safe to redirect 300 is missing a location
        // header, then it will be returned as the 300 result as well
        switch (result.statusCode) {
          case 301:
          case 302:
          case 303:
            if (req.method === 'GET' || req.method === 'HEAD') {
              let location = result.headers.location;
              if (location && currentRedirect < maxRedirects) {
                return await(this.run({
                  req: {
                    url: location,
                    method: req.method,
                    headers: req.headers,
                  },
                  body,
                  streamingOutput,
                  followRedirect,
                  maxRedirects,
                  currentRedirect: currentRedirect + 1,
                }));
              } else {
                debug(`WARNING: will only follow ${result.statusCode} with Location header`);
              }
            } else {
              debug(`WARNING: will only follow ${result.statusCode} for GET or HEAD`);
            }
            break;
          default:
            debug('WARNING: will only follow a 301, 302 or 303 redirect');
        }
      }
      return result;
    }
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
  async runOnce(opts) {
    let {req, body, streamingOutput} = opts;

    InterchangeFormat.validate(req);

    let {url, method, headers} = req;

    method = method.toUpperCase();

    body = body || '';
    let streamingInput;

    // Some sanity checking done
    if (body) {
      if (method === 'GET' || method === 'HEAD') {
        throw new Error('It is a violation of HTTP for GET or HEAD to have body');
      }

      if (typeof body === 'string' || body instanceof Buffer) {
        streamingInput = false;
      } else if (typeof body === 'function') {
        streamingInput = true;
        body = body();
      } else if (typeof body.pipe === 'function') {
        streamingInput = true;
      } else {
        throw new Error('Received an unexpected type of body');
      }
    }

    async function streamingRequest(request, body, responseHandler) {
      return new Promise((resolve, reject) => {
        let digestStream = new DigestStream();

        request.on('error', reject);

        // We want to make sure that we abort requests for which there
        // was an error reading the body
        body.on('error', err => {
          request.abort();
          reject(err);
        });

        digestStream.on('error', err => {
          request.abort();
          reject(err);
        });

        digestStream.on('end', () => {
          let debugStr = `COMPLETE ${method} ${url}`;
          if (request.headers) {
            debugStr += ` HEADERS: ${JSON.stringify(headers)}`;
          }
          debugStr += ` ${digestStream.hash} ${digestStream.size} bytes`;
          debugRequest(debugStr);
          request.end();
        });

        request.on('response', response => {
          resolve(responseHandler(response));
        });

        body.pipe(digestStream).pipe(request);
      });
    }

    async function inMemoryRequest(request, body, reponseHandler) {
      return new Promise((resolve, reject) => {
        let requestHash = crypto.createHash('sha256').update(body);
        let requestSize = body.length;

        request.on('response', response => {
          resolve(responseHandler(response));
        });

        let debugStr = `COMPLETE ${method} ${url}`;
        if (request.headers) {
          debugStr += ` HEADERS: ${JSON.stringify(headers)}`;
        }

        if (body) {
          request.write(body);
          debugStr += ` ${requestHash.digest('hex')} ${body.length} bytes`;
        }

        debugRequest(debugStr);

        request.end();
      });
    }

    async function streamingResponse(response) {
      return new Promise((resolve, reject) => {

        let debugStr = `RESPONSE (streaming) ${response.statusCode} `
        debugStr += `${response.statusMessage} ${method} ${url}`;
        if (headers) {
          debugStr += ` HEADERS: ${JSON.stringify(headers)}`;
        }
        // Rather than piping this though a DigestStream, for now, I'd like to
        // just let the downstream consumer do any hashing it chooses to
        //debugStr += ` ${responseHash.digest('hex')} ${body.length} bytes`;
        debugResponse(debugStr);

        let headers = response.headers;
        let statusCode = response.statusCode;
        let statusMessage = response.statusMessage;

        resolve({
          bodyStream: response,
          headers,
          statusCode,
          statusMessage,
        });
      });
    }

    async function inMemoryResponse(response) {
      return new Promise((resolve, reject) => {
        let responseData = [];
        let responseHash = crypto.createHash('sha256').update('');

        response.on('data', data => {
          responseData.push(data);
          responseHash.update(data);
        });

        response.on('end', () => {
          try {
            let body = Buffer.concat(responseData);
            let headers = response.headers;
            let statusCode = response.statusCode;
            let statusMessage = response.statusMessage;

            let debugStr = `RESPONSE ${statusCode} ${statusMessage} ${method} ${url}`;
            if (headers) {
              debugStr += ` HEADERS: ${JSON.stringify(headers)}`;
            }
            debugStr += ` ${responseHash.digest('hex')} ${body.length} bytes`;
            debugResponse(debugStr);

            resolve({body, headers, statusCode, statusMessage});
          } catch (err) {
            reject(err);
          }
        });

      });
    }

    let _request = urllib.parse(url);
    _request.method = method;
    _request.headers = headers;

    let request;
    if (_request.protocol !== 'http:') {
      request = https.request(_request);
    } else {
      request = http.request(_request);
    }

    let debugStr = `REQUESTED ${method} ${url}`;

    if (request.headers) {
      debugStr += ` HEADERS: ${JSON.stringify(headers)}`;
    }

    debugRequest(debugStr);
    request.on('aborted', () => {
      request.emit('error', new Error('Server Hangup'));
    });

    let requestHandler = streamingInput ? streamingRequest : inMemoryRequest;
    let responseHandler = streamingOutput ? streamingResponse : inMemoryResponse;

    return requestHandler(request, body, responseHandler);
  }
}

Runner.returnSchema = Joi.object().keys({
  body: Joi.object().type(Buffer),
  bodyStream: Joi.object().type(stream.Readable),
  headers: Joi.object().required(),
  statusCode: Joi.number().integer().min(100).max(599).required(),
  statusMessage: Joi.string().required(),
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
