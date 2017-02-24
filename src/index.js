import aws4 from 'aws4';

import http from 'http';
import https from 'https';
import urllib from 'url';
import _debug from 'debug';
import DigestStream from './digest-stream';
import crypto from 'crypto';

import libxml from 'libxmljs';

let debug = _debug('remote-s3');

/**
 * This is a reduced scope S3 client which knows how to run the following
 * requests:
 *    1. Initiate a multipart upload locally
 *    2. Generate an object describing the URL, Method and Headers that a remote
 *       machine must use to upload an individual part of a multi-part upload
 *    3. Complete a multipart upload
 *    4. Abort a multipart upload
 *    5. Generate an object describing the URL, Method and Headers that a remote
 *       machine must use to upload a single-part upload
 *
 * NOTE: In all cases when there is a list of parts, the order of the list implies
 * the part number.  S3 uses 1-based counting counter to Javascripts 0-based
 * indexing.  This means that PartNumber === JS-Index + 1
 */
class S3 {
  constructor(region, runner) {
    this.region = region;
    this.runner = runner;
    // http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
    let s3region = region === 'us-east-1' ? 's3' : 's3-' + region;
    this.s3host = `${s3region}.amazonaws.com`;
  }

  /** Convert the result from the aws4.sign method into the
   * general form and return an object in the form:
   *   { url: '...', method: '...', headers: {key: 'value'}}
   */
  __serializeRequest(req) {
    return {
      url: `${req.protocol}//${req.hostname}${req.path}`,
      method: req.method,
      headers: req.headers,
    };
  }

  /**
   * Generate the XML body required to mark a Multipart Upload as completed.
   *
   * This function takes an ordered list of Etag values and generates the XML
   * Body that S3 expects.  The PartNumber will be the index of the supplied
   * list + 1 to account for the 1-based numbering of PartNumbers
   *
   * EXAMPLE:
   * <CompleteMultipartUpload>
   *   <Part>
   *     <PartNumber>PartNumber</PartNumber>
   *     <ETag>ETag</ETag>
   *   </Part>
   * </CompleteMultipartUpload>
   */
  __generateCompleteUploadBody(etags) {
    let doc = new libxml.Document();

    let ctx = doc.node('CompleteMultipartUpload');
    for (let x = 0; x < etags.length; x++) {
      ctx = ctx.node('Part');
      ctx = ctx.node('PartNumber', Number(x+1).toString());
      ctx = ctx.parent();
      ctx = ctx.node('ETag', etags[x]);
      ctx = ctx.parent();
      ctx = ctx.parent();
    }

    return doc.toString();
  }

  /**
   * Obtain an UploadId from an Initiate Multipart Upload response body
   */
  __getUploadId(doc, bucket, key) {
    if (doc.root().name() !== 'InitiateMultipartUploadResult') {
      throw new Error('Document is not an InitiateMultipartUploadResult');
    }

    let uploadId, foundBucket, foundKey;
    
    for (let child of doc.root().childNodes()) {
      switch (child.name()) {
        case 'Bucket':
          foundBucket = child.text();
          break;
        case 'Key':
          foundKey = child.text();
          break;
        case 'UploadId':
          uploadId = child.text();
          break;
      }
    }

    if (foundBucket !== bucket) {
      throw new Error('Document contains incorrect Bucket');
    }

    if (foundKey !== key) {
      throw new Error('Document contains incorrect Key');
    }

    if (!uploadId) {
      throw new Error('Document does not contain UploadId');
    }

    return uploadId;
  }
 
  /**
   * Initiate a Multipart upload and return the UploadIp that
   * Amazon has assigned for this multipart upload
   * http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadInitiate.html
   */
  async initiateMultipartUpload(bucket, key, sha256, size) {
    let signedRequest = aws4.sign({
      service: 's3',
      region: this.region,
      method: 'POST',
      protocol: 'https:',
      hostname: `${bucket}.${this.s3host}`,
      path: `/${key}?uploads=`,
      headers: {
        'x-amz-meta-taskcluster-content-sha256': sha256,
        'x-amz-meta-taskcluster-content-length': size,
      }
    });


    let response = await this.runner(this.__serializeRequest(signedRequest));
    if (response.statusCode !== 200) {
      throw new Error('Expected HTTP Status Code 200, got: ' + response.statusCode);
    }
    let uploadId = this.__getUploadId(parseS3Response(response.body), bucket, key);
    return uploadId;
  }

  /**
   * Generate the general request data for uploading an individual part of a
   * multipart upload.  The uploadId must be provided from a previous call to
   * the S3 initate multipart API.  The parts argument is a list of objects
   * describing the S3 upload parts.  They should be in the format {sha256:
   * '...', offset: int, size: int}
   *
   * NOTE: It is required that the process that eventually runs this request
   * parse the response to get the Etag for the purpose of completing the
   * upload
   *
   * http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html
   */
  async generateMultipartRequest(bucket, key, uploadId, parts) {
    let requests = [];
    for (let num = 1 ; num <= parts.length ; num++) {
      let signedRequest = aws4.sign({
        service: 's3',
        region: this.region,
        method: 'PUT',
        protocol: 'https:',
        hostname: `${bucket}.${this.s3host}`,
        path: `/${key}?partNumber=${num}&uploadId=${uploadId}`,
        headers: {
          'x-amz-content-sha256': parts[num - 1].sha256,
          'content-length': parts[num - 1].size,
        }
      });

      requests.push(this.__serializeRequest(signedRequest));
    }

    return requests;
  }

  /**
   * Mark a multipart upload as completed. 
   *
   * http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadComplete.html
   */
  async completeMultipartUpload(bucket, key, uploadId, etags) {
    let requestBody = this.__generateCompleteUploadBody(etags);
    let signedRequest = aws4.sign({
      service: 's3',
      region: this.region,
      method: 'POST',
      protocol: 'https:',
      hostname: `${bucket}.${this.s3host}`,
      path: `/${key}?uploadId=${uploadId}`,
      body: requestBody,
    });

    let response = await this.runner(this.__serializeRequest(signedRequest), requestBody);
    if (response.statusCode !== 200) {
      throw new Error('Expected HTTP Status Code 200, got: ' + response.statusCode);
    }
  }

  // http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadAbort.html
  async abortMultipartUpload(bucket, key, uploadId) {
    let signedRequest = aws4.sign({
      service: 's3',
      region: this.region,
      method: 'DELETE',
      protocol: 'https:',
      hostname: `${bucket}.${this.s3host}`,
      path: `/${key}?uploads=`,
    });

    let response = await this.runner(this.__serializeRequest(signedRequest));
    if (response.statusCode !== 204) {
      throw new Error('Expected HTTP Status Code 204, got: ' + response.statusCode);
    }
  }

  /**
   * Generate the general request for uploading a resource to S3
   * in a single request.
   *
   * NOTE: We still set the x-amz-meta-taskcluster-content-length because
   * the multipart uploaded things must have this value set there as well.
   *
   * http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html
   */
  async generateSinglepartRequest(bucket, key, size, sha256) {
    let signedRequest = aws4.sign({
      service: 's3',
      region: this.region,
      method: 'PUT',
      protocol: 'https:',
      hostname: `${bucket}.${this.s3host}`,
      path: `/${key}`,
      headers: {
        'content-length': size,
        'x-amz-content-sha256': sha256,
        'x-amz-meta-taskcluster-content-sha256': sha256,
        'x-amz-meta-taskcluster-content-length': size,
      }
    });

    return this.__serializeRequest(signedRequest);
  }
}

/**
 * Check a response body for an S3 api error and throw it if present.
 * If there is no response return either `undefined` for an empty
 * body or an interface to the result of XML parsing
 *
 * http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
 */
function parseS3Response(body, noThrow = false) {
  if (!body) {
    return undefined;
  }

  let doc = libxml.parseXml(body);
  
  // For Errors, we want to make sure that all the error properties
  // are exposed
  if (doc.root().name() === 'Error') {
    let errorProperties = {};
    for (let child of doc.root().childNodes()) {
      errorProperties[child.name()] = child.text();
    }
    let error = new Error(errorProperties.Message || 'Unknown S3 Error');
    for (let property in errorProperties) {
      error[property.toLowerCase()] = errorProperties[property];
    }

    if (!error.code) {
      error.code = 'UnknownError';
    }

    if (noThrow) {
      return error;
    } else {
      throw error;
    }
  }

  return doc;
}

/**
 * Run a generic request using information return by the S3 class in this
 * module.
 */
async function run(generalRequest, body, noThrow) {
  let {url, method, headers} = generalRequest;

  if (body) {
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
        `Request body ${requestHash.digest('hex')} (${requestSize} bytes)`,
        `Response body ${responseHash.digest('hex')} (${responseSize} bytes)`,
      ].join('\n');
      debug(string);
      return _reject(err);
    }

    let parts = urllib.parse(url);
    parts.method = method.toUpperCase();
    parts.headers = headers;


    let request = https.request(parts);

    request.on('error', reject);

    request.on('response', response => {
      let responseHash = crypto.createHash('sha256');
      let responseSize = 0;
      let responseChunks = [];

      response.on('error', reject);

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
          let responseBody = Buffer.concat(responseChunks);
          let string = [
            `SUCCESS ${method} "${url}" `,
            `REQ: ${requestHash.digest('hex')} (${requestSize} bytes) `,
            `RES: ${responseHash.digest('hex')} (${responseSize} bytes)`,
          ].join('');
          debug(string);

          resolve(responseBody.toString());
        } catch (err) {
          reject(err);
        }
      });
    });

    if (body) {
      if (typeof body === 'string' || body instanceof Buffer) {
        requestHash.update(body);
        requestSize = body.length;
        request.write(body);
      } else if (typeof body.pipe === 'function') {

      } 
    } else {
      request.end();
    }

  });
  //return {body: '', headers: '', statusCode: 200};
}

module.exports = {
  S3,
  run,
  DigestStream,
  parseS3Response
};
