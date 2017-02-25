import http from 'http';
import https from 'https';
import crypto from 'crypto';

import _debug from 'debug';
import aws4 from 'aws4';
import libxml from 'libxmljs';

import Runner from './runner';

const debug = _debug('remote-s3:Bucket');

// We want to ensure that any where that we're doign things involving
// SHA256 Hex digests that they are a valid format
const emptyStringSha256 = crypto.createHash('sha256').update('').digest('hex');
function validateSha256 (sha256) {
  if (sha256 === emptyStringSha256) {
    throw new Error('SHA256 values must not be of the empty string');
  } else if (!/^[a-fA-F0-9]{64}$/.test(sha256)) {
    throw new Error('SHA256 is not a valid format');
  }
}

/**
 * This is a reduced scope S3 client which knows how to run the following
 *
 * Requests:
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
class Controller {
  constructor(region, runner) {
    this.region = region;
    if (!runner) {
      runner = new Runner();
    }
    this.runner = runner.run;
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
   * General method for extracting a specific property from an S3
   * response.  This assumes it's a top level node in the main container,
   * the response has a Bucket and Key property and those match
   * the passed in ones.  This is a sanity check.
   */
  __getResponseProperty(doc, container, property, bucket, key) {
    if (doc.root().name() !== container) {
      throw new Error('Document does not have ' + container);
    }

    let lookingfor, foundBucket, foundKey;
    
    for (let child of doc.root().childNodes()) {
      switch (child.name()) {
        case 'Bucket':
          foundBucket = child.text();
          break;
        case 'Key':
          foundKey = child.text();
          break;
        case property:
          lookingfor = child.text();
          break;
      }
    }

    if (foundBucket !== bucket) {
      throw new Error('Document contains incorrect Bucket');
    }

    if (foundKey !== key) {
      throw new Error('Document contains incorrect Key');
    }

    if (typeof lookingfor === 'undefined') {
      throw new Error('Document does not contain ' + property);
    }

    return lookingfor;
  }
  /**
   * Obtain an UploadId from an Initiate Multipart Upload response body
   */
  __getUploadId(doc, bucket, key) {
    return this.__getResponseProperty(doc, 'InitiateMultipartUploadResult', 'UploadId', bucket, key);
  }

  /**
   * Obtain the ETag for the committed multipart upload
   */
  __getMultipartEtag(doc, bucket, key) {
    return this.__getResponseProperty(doc, 'CompleteMultipartUploadResult', 'ETag', bucket, key);
  }
 
  /**
   * Initiate a Multipart upload and return the UploadIp that
   * Amazon has assigned for this multipart upload
   * http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadInitiate.html
   */
  async initiateMultipartUpload(opts) {
    let {bucket, key, sha256, size} = opts;
    validateSha256(sha256);
    if (size <= 0) {
      throw new Error('Objects must be more than 0 bytes');
    }
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
  async generateMultipartRequest(opts) {
    let {bucket, key, uploadId, parts} = opts;
    let requests = [];
    for (let num = 1 ; num <= parts.length ; num++) {
      let part = parts[num - 1];

      validateSha256(part.sha256);
      if (part.size <= 0) {
        throw new Error(`Part ${num} must be more than 0 bytes`);
      }

      let signedRequest = aws4.sign({
        service: 's3',
        region: this.region,
        method: 'PUT',
        protocol: 'https:',
        hostname: `${bucket}.${this.s3host}`,
        path: `/${key}?partNumber=${num}&uploadId=${uploadId}`,
        headers: {
          'x-amz-content-sha256': part.sha256,
          'content-length': part.size,
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
  async completeMultipartUpload(opts) {
    let {bucket, key, uploadId, etags} = opts;
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
    return this.__getMultipartEtag(parseS3Response(response.body), bucket, key);
  }

  // http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadAbort.html
  async abortMultipartUpload(opts) {
    let {bucket, key, uploadId} = opts;
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
  async generateSinglepartRequest(opts) {
    let {bucket, key, sha256, size} = opts;
    !validateSha256(sha256);
    if (size <= 0) {
      throw new Error('Objects must be more than 0 bytes');
    }
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

module.exports = {
  Controller,
  parseS3Response
};
