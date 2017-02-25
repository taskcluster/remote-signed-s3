import crypto from 'crypto';
import https from 'https';
import urllib from 'url';

import _debug from 'debug';

import DigestStream from './digest-stream';

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
  async run(req, body) {
    let {url, method, headers} = req;

    method = method.toUpperCase();

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

      request.on('error', reject);

      request.on('finish', () => {
        let str = `${method} ${url}`;
        if (Object.keys(headers).length > 0) {
          str += ` ${JSON.stringify(headers)}`;
        }
        if (requestHash && requestSize > 0) {
          str += ` ${requestHash} ${requestSize} bytes`;
        }
        debugRequest(str);
      });

      request.on('response', response => {
        let statusCode = response.statusCode;
        let statusMsg = response.statusMessage;
        let responseChunks = [];

        response.on('error', reject);

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
          responseHash = responseHash.digest('hex');
          try {
            let string = [];

            if (statusCode >= 200 && statusCode < 300) {
              string.push('SUCCESS: ');
            } else {
              string.push('ERROR: ');
            }

            string.push([`${statusCode} ${statusMsg} ${method} "${url}"`]);
            let responseBody = Buffer.concat(responseChunks);

            if (requestSize > 0) {
              string.push(` REQ: ${requestHash} (${requestSize} bytes)`);
            } else {
              string.push(' REQ: empty');
            }
            if (responseSize > 0) {
              string.push(` RES: ${responseHash} (${responseSize} bytes)`);
            } else {
              string.push(' REQ: empty');
            }
            
            string = string.join('');
            //debugResponse(string);

            resolve({
              body: responseBody,
              headers: response.headers,
              statusCode,
              statusMessage: statusMsg,
              requestHash,
              requestSize,
              responseHash,
              responseSize
            });

          } catch (err) {
            reject(err);
          }
        });
      });

      if (body) {
        if (typeof body === 'string' || body instanceof Buffer) {
          requestHash = crypto
            .createHash('sha256')
            .update(body)
            .digest('hex');

          requestSize = body.length;

          request.write(body);
          request.end();
        } else if (typeof body.pipe === 'function' || typeof body === 'function') {
          // Remember that the body could be a function which returns a stream.
          // This is useful in the case of retrying since I'm pretty sure that
          // node's fs.createReadStream() cannot seek().
          let bodyStream = typeof body === 'function' ? body() : body;
          let digestStream = new DigestStream();
          digestStream.on('end', () => {
            requestHash = digestStream.hash;
            requestSize = digestStream.size;
            request.end();
          });
          bodyStream.pipe(digestStream).pipe(request);
        } 
      } else {
        requestSize = 0;
        request.end();
      }

    });
  }
}

module.exports = Runner;
