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

  async run(opts) {
    return this.runOnce(opts);
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

    await InterchangeFormat.validate(req);

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

    let request = https.request(_request);

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
