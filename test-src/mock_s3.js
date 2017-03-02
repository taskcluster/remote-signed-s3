const crypto = require('crypto');
const http = require('http');
//const https = require('https');
const querystring = require('querystring');
const urllib = require('url');
const fs = require('fs');

const { Controller, parseS3Response } = require('../lib/controller');
/**
 * This is a special server that roughly mocks out the Amazon
 * S3 api.  It is not exhaustive but does the following checks:
 *   - is the x-amz-content-sha256 header computed correctly
 *   - is the content-length header correct
 *   - does the bucket and key match what we expect it to
 *   - does an initate multipart body parse successfully
 *   - does a complete multipart body parse successfully
 *   - is the http method correct
 *   - do all query string paramters match the expected value
 *
 * When it finds that a request that does not match expectations it emits an
 * event 'unittest-failure' with a list of string values for the errors.  If
 * everything is good from its perspective then it will emit an event
 * 'unittest-success' with no arguments.
 *
 * It will generate responses which look like the examples from the S3 API
 * Docs.  A couple headers are generated to make sure that the response does
 * match the request, like the key and bucket.  It will parse the input of each
 * request using the already-tested parsing logic of the Controller and ensure
 * that the input is valid.
 *
 * It will only give a valid S3-esque response if there are no failures.  If
 * there are failures, beyond the event being emitted a response body will be
 * given with a 418 I'M A TEAPOT error, with the Unitest: failure header set
 * and a JSON body which will contain information on the error 
 */
async function createMockS3Server(opts) {
  let {bucket, key, callIfFail, requestType, maxRequests, port} = opts;

  // we're going to use this to parse input bodies
  // and we know that works because we have parsing tests
  // in the test_xml.js file
  let _controller = new Controller();

  maxRequests = maxRequests || 1;

  // These are the request types we understand
  let requestTypes = [
    'initiateMPUpload',
    'uploadPart',
    'uploadObject',
    'completeMPUpload',
    'abortMPUpload',
    'tagObject',
    'generate200Error',
    'generate400Error',
  ];

  if (requestTypes.indexOf(requestType) === -1) {
    throw new Error('This is not a known request type: ' + requestType);
  }


  let server = http.createServer();
  let requestNumber = 0;

  server.on('request', (request, response) => {
    try {
      let failures = [];
      requestNumber++;
      if (requestNumber > maxRequests) {
        failures.push('exceeded max allowed requests');
      }

      let size = 0;
      let hash = crypto.createHash('sha256');
      let requestBody = [];


      request.on('data', data => {
        try {
          hash.update(data);
          size += data.length;
          requestBody.push(data);
        } catch (err) {
          console.log(err.stack || err);
          throw err;
        }
      });

      request.on('end', () => {
        try {
          hash = hash.digest('hex');
          requestBody = Buffer.concat(requestBody);

          // First, let's check a couple things to make sure that the request is
          // valid.  We know that the content-sha256 must exist for the AWS calls
          // we're checking here.  This is a common source of errors in AWS4
          // signing
          if (request.headers['x-amz-content-sha256'] !== hash) {
            failures.push('x-amz-content-sha256 mismatch');
          }
          if (Number.parseInt(request.headers['content-length']) !== size) {
            failures.push('content-length mismatch');
          }

          // Let's figure out the S3 specific stuff
          let requestDotUrl = urllib.parse(request.url);
          let requestKey = requestDotUrl.pathname.slice(1);
          let requestHost = request.headers.host;
          let requestBucket = requestHost.split('.')[0];
          let requestOptions = querystring.parse(requestDotUrl.query);

          // Now let's evaluate the data!
          if (requestBucket !== bucket) {
            failures.push('Bucket Mismatch');
          }
          if (requestKey !== key) {
            failures.push('Key Mismatch');
          }
          
          // Now let's do some S3 magic
          let statusCode;
          let statusMessage;
          let body;
          let headers = {
            unittest: 'success',
          };

          // Values for the responses are generated based on the
          // values from the S3 Rest API doc sample responses.
          // Where possible, the exact response is used.  Changes
          // are only to match the buckets and keys we're using
          switch (requestType) {
            /////////////////////////////////////////////////////////////
            case 'initiateMPUpload':          
              if (request.method !== 'POST') {
                failures.push('incorrect http method');
              }
              if (requestOptions.uploads !== '') {
                failures.push('incorrect or missing uploads= in qs');
              }
              try {
                parseS3Response(requestBody, bucket, key);
              } catch(err) {
                failures.push(err);
              }
              statusCode = 200;
              statusMessage = 'OK';
              body = [
                '<?xml version="1.0" encoding="UTF-8"?>',
                '<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
                '  <Bucket>' + bucket + '</Bucket>',
                '  <Key>' + key + '</Key>',
                '  <UploadId>VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>',
                '</InitiateMultipartUploadResult>',
              ].join('\n');
              headers = {
                'x-amz-id-2': 'Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==',
                'x-amz-request-id': '656c76696e6727732072657175657374',
                Date: new Date().toGMTString(),
                'Content-Length': body.length,
                Connection: 'keep-alive',
                Server: 'AmazonS3',
              }
              break;
            /////////////////////////////////////////////////////////////
            case 'uploadPart':
            case 'uploadObject':
              failures.push('uploading parts not yet implemented');
              break;
            /////////////////////////////////////////////////////////////
            case 'completeMPUpload':
              if (request.method !== 'POST') {
                failures.push('incorrect http method');
              }
              if (typeof requestOptions.uploadId !== 'string') {
                failures.push('incorrect or missing uploadId= in qs');
              }
              try {
                let doc = parseS3Response(requestBody, bucket, key);
              } catch(err) {
                failures.push(err);
              }
              statusCode = 200;
              statusMessage = 'OK'
              body = [
                '<?xml version="1.0" encoding="UTF-8"?>',
                '<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
                `  <Location>http://${bucket}.s3.amazonaws.com/${key}</Location>`,
                `  <Bucket>${bucket}</Bucket>`,
                `  <Key>${key}</Key>`,
                '  <ETag>"3858f62230ac3c915f300c664312c11f-9"</ETag>',
                '</CompleteMultipartUploadResult>',
              ].join('\n');
              headers = {
                'x-amz-id-2': 'Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==',
                'x-amz-request-id': '656c76696e6727732072657175657374',
                Date: new Date().toGMTString(),
                'Content-Length': body.length,
                Connection: 'close',
                Server: 'AmazonS3',
              }
              break;
            /////////////////////////////////////////////////////////////
            case 'abortMPUpload':
              if (request.method !== 'DELETE') {
                failures.push('incorrect http method');
              }
              if (typeof requestOptions.uploadId !== 'string') {
                failures.push('incorrect or missing uploadId= in qs');
              }
              if (requestBody.toString() !== '') {
                failures.push('Expected abort upload body to be empty');
              }
              statusCode = 204;
              statusMessage = 'OK';
              body = '';
              headers = {
                'x-amz-id-2': 'Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==',
                'x-amz-request-id': '656c76696e6727732072657175657374',
                Date: new Date().toGMTString(),
                'Content-Length': body.length,
                Connection: 'keep-alive',
                Server: 'AmazonS3',       
              }
              break;
            /////////////////////////////////////////////////////////////
            case 'generate200Error':
              statusCode = 200;
              statusMessage = 'OK';
              body = [
                '<Error>',
                '  <Code>InternalError</Code>',
                '  <Message>We encountered an internal error. Please try again.</Message>',
                '  <RequestId>656c76696e6727732072657175657374</RequestId>',
                '  <HostId>Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==</HostId>',
                '</Error>',
              ].join('\n');
              headers = {
                'x-amz-id-2': 'Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==',
                'x-amz-request-id': '656c76696e6727732072657175657374',
                Date: new Date().toGMTString(),
                'Content-Length': body.length,
                Connection: 'keep-alive',
                Server: 'AmazonS3',       
              }
            /////////////////////////////////////////////////////////////
            case 'generate400Error':
              // NOTE: THE FALLTHROUGH OF THE SWITCH
              statusCode = 403;
              statusMessage = 'Forbidden';
              break;
            case 'tagObject':
              if (request.method !== 'PUT') {
                failures.push('incorrect http method');
              }
              if (requestOptions.tagging !== '') {
                failures.push('incorrect or missing tagging= in qs');
              }
              statusCode = 200;
              statusMessage = 'OK',
              body = '';
              headers = {
                'x-amz-id-2': 'Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==',
                'x-amz-request-id': '656c76696e6727732072657175657374',
                Date: new Date().toGMTString(),
              };
              break;
            /////////////////////////////////////////////////////////////
            defaut:
              failures.push('This is not a supported S3 operation');
          }


          if (failures.length > 0) {
            server.emit('unittest-failure', 
              new Error(JSON.stringify(failures.map(x => {
                return x.stack || x; 
              }), null, 2)));
            response.writeHead(418, 'I\'M A TEAPOT', {
              unittest: 'failure',
              reasons: failures.length,
              'content-type': 'application/json',
            });
            response.end(JSON.stringify({
              outcome: 'failure',
              failures: failures.map(x => x.stack || x),
              responseAttemptedValues: {
                statusCode,
                statusMessage,
                body,
                headers,
              }
            }, null, 2));
            if (typeof callIfFail === 'function') {
              callIfFail();
            }
          } else {
            server.emit('unittest-success');
            response.writeHead(statusCode, statusMessage, headers);
            response.end(body);
          }
        } catch (err) {
          console.log(err.stack || err);
          throw err;
        }
      });
    } catch (err) {
      response.writeHead(500, 'MOCK S3 FAILURE');
      response.end(err.stack || err);
      console.log(err.stack || err);
      throw err;
    }
  });

  // Ensure that the server has started up before continuing
  await new Promise((resolve, reject) => {
    server.listen(port, 'localhost', () => {
      console.log('listening on ' + port);
      resolve();
    })
  });

  return server;
}
module.exports = createMockS3Server;
