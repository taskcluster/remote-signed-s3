import aws4 from 'aws4';

import http from 'http';
import https from 'https';
import urllib from 'url';
import _debug from 'debug';

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
  constructor(region) {
    this.region = region;
    let s3region = region === 'us-east-1' ? '' : '.' + region;
    this.s3host = `s3${s3region}.amazonaws.com`;
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
   */
  __generateCompleteUploadBody(etags) {
    return ''; 
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

    let response = await run(this.__serializeRequest(signedRequest));
    //let uploadId = response.getFromXml('UploadId');
    let uploadId = '<placeholder>';
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
        }
      });

      requests.push(__this.serializeRequest(signedRequest));
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
      path: `/${key}?uploads=`,
      headers: {
        'x-amz-meta-taskcluster-content-sha256': sha256,
        'x-amz-meta-taskcluster-content-length': size,
      },
      body: requestBody,
    });

    await run(this.__serializeRequest(signedRequest), requestBody);
  }

  // http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadAbort.html
  async abortMultipartUpload(bucket, key, uploadId) {
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

    await run(this.__serializeRequest(signedRequest));
  }

  /**
   * Generate the general request for uploading a resource to S3
   * in a single request.
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
      path: `/${key}?uploads=`,
      headers: {
        'content-length': size,
        'x-amz-content-sha256': sha256,
        'x-amz-meta-taskcluster-content-sha256': sha256,
        'x-amz-meta-taskcluster-content-length': size,
      }
    });
  }
}

/**
 * Check a response body for an S3 api error and throw it if present.
 * If there is no response return either `undefined` for an empty
 * body or an interface to the result of XML parsing
 */
function parseS3Response(body) {
  return;
}

/**
 * Run a generic request using information return by the S3 class in this
 * module.
 */
async function run(method, url, headers, body = '') {
  debug(`${method} ${url} ${JSON.stringify(headers)}`);
  // make sure to call parseS3Response and return the value from that
}

module.exports = {
  S3,
  run,
  parseS3Response
};
