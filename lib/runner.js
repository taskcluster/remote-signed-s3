'use strict';
const crypto = require('crypto');
const http = require('http');
const https = require('https');
const urllib = require('url');
const stream = require('stream');
const fs = require('fs');

const _debug = require('debug');
const {Joi, schemas, runSchema} = require('./schemas');

const DigestStream = require('./digest-stream');
const InterchangeFormat = require('./interchange-format');

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
    let {req, body, streamingOutput} = opts;
    let current = 0;

    // Certain types of request bodies are not replayable.  For these types of
    // bodies, we want to support making a single request but we will notify
    // the user and we will disable the ability to do any form of automatic
    // retries
    if (body && body instanceof stream.Readable || body && typeof body.pipe === 'function') {
      debug('WARNING: instances of stream.Readable are not retryable');
      current = this.maxRetries - 1;
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
          let debugStr = `COMPLETE (streaming) ${method} ${url}`;
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

        let debugStr = `COMPLETE (buffered) ${method} ${url}`;
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
        let headers = response.headers;
        let statusCode = response.statusCode;
        let statusMessage = response.statusMessage;

        let debugStr = `RESPONSE (streaming) ${response.statusCode} `
        debugStr += `${response.statusMessage} ${method} ${url}`;
        if (headers) {
          debugStr += ` HEADERS: ${JSON.stringify(headers)}`;
        }
        // Rather than piping this though a DigestStream, for now, I'd like to
        // just let the downstream consumer do any hashing it chooses to
        //debugStr += ` ${responseHash.digest('hex')} ${body.length} bytes`;
        debugResponse(debugStr);

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

        response.on('error', reject);

        response.on('data', data => {
          try {
            responseData.push(data);
            responseHash.update(data);
          } catch (err) {
            reject(err);
          }
        });

        response.on('end', () => {
          try {
            let body = Buffer.concat(responseData);
            let headers = response.headers;
            let statusCode = response.statusCode;
            let statusMessage = response.statusMessage;

            let debugStr = `RESPONSE (buffered) ${statusCode} ${statusMessage} ${method} ${url}`;
            if (headers) {
              debugStr += ` HEADERS: ${JSON.stringify(headers)}`;
            }
            debugStr += ` ${responseHash.digest('hex')} ${body.length} bytes`;
            debugStr += `\n ${body.slice(0,1024)}`;
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
      // The following section of code is commented out but left in place so
      // that we don't have to look it up each time we need to use it.  This is
      // used to be able to listen in on HTTPS conversations using Wireshark.
      //request.once('socket', (s) => {
      //  s.once('secureConnect', function secureConnectOnce() {
      //    let session = s.getSession();
      //    let sessionId = session.slice(17, 17+32).toString('hex');
      //    let masterKey = session.slice(51, 51+48).toString('hex');
      //    fs.appendFileSync('sslkeylog.log', `RSA Session-ID:${sessionId} Master-Key:${masterKey}\n`);
      //  });
      //});
    } else {
      request = http.request(_request);
    }

    let debugStr = `REQUESTED ${method} ${url}`;

    if (_request.headers) {
      debugStr += ` HEADERS: ${JSON.stringify(_request.headers)}`;
    }

    debugRequest(debugStr);

    request.on('aborted', () => {
      request.emit('error', new Error('Server Hangup'));
    });

    request.on('continue', () =>  {
      debugRequest('Received a 100-Continue');
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
