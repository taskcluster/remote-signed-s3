import crypto from 'crypto';
import https from 'https';
import urllib from 'url';

import _debug from 'debug';

import DigestStream from './digest-stream';

const debug = _debug('remote-s3:Runner');

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

  async run(req, body, noThrow) {
    let {url, method, headers} = req;
    method = method.toUpperCase();

    if (body) {
      if (method === 'GET' || method === 'HEAD') {
        throw new Error('It is a violation of HTTP for GET or HEAD to have body');
      }

      if (typeof body !== 'string' && typeof body.pipe !== 'function') {
        throw new Error('If provided, body must be string or readable stream');
      }
    }

    return new Promise((resolve, _reject) => {
      // We need to parse the URL for the basis of our request options
      // for the actual HTTP request
      let requestHash = crypto.createHash('sha256');
      let requestSize = 0;
      let responseHash = crypto.createHash('sha256');
      let responseSize = 0;

      function reject(err) {
        let string = [
          'ERROR: ' + err,
          `${method} ${url}`,
          `Headers: ${JSON.stringify(headers, null, 2)}`,
          `Request body ${requestHash} (${requestSize} bytes)`,
          `Response body ${responseHash} (${responseSize} bytes)`,
        ].join('\n');
        debug(string);
        return _reject(err);
      }

      let parts = urllib.parse(url);
      parts.method = method;
      parts.headers = headers;
      let request = https.request(parts);



      request.on('error', reject);

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
            let error = false;

            if (statusCode >= 200 && statusCode < 300) {
              string.push('SUCCESS: ');
            } else {
              string.push('ERROR: ');
              error = true;
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
            debug(string);

            if (error) {
              let err = new Error('Error running General HTTP Request');
              err.url = url;
              err.headers = headers;
              err.method = method;
              err.statusCode = statusCode;
              if (!noThrow) {
                throw err;
              }
            } else {
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
            }

          } catch (err) {
            reject(err);
          }
        });
      });

      if (body) {
        if (typeof body === 'string' || body instanceof Buffer) {
          requestHash.update(body);
          requestHash = requestHash.digest('hex');
          requestSize = body.length;
          request.write(body);
          request.end();
        } else if (typeof body.pipe === 'function') {
          let ds = new DigestStream();
          ds.on('end', () => {
            requestHash = ds.hash;
            request.end();
          });
          body.pipe(ds).pipe(request);
        } 
      } else {
        request.end();
      }

    });
  }
}

module.exports = Runner;
