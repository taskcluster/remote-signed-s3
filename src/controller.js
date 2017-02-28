import http from 'http';
import https from 'https';
import crypto from 'crypto';
import qs from 'querystring';

import _debug from 'debug';
import aws4 from 'aws4';
import libxml from 'libxmljs';

import Runner from './runner';
import InterchangeFormat from './interchange-format';

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

// This the list of canned ACLs that are valid.
const cannedACLs = [
  'private',
  'public-read',
  'public-read-write',
  'aws-exec-read',
  'authenticated-read',
  'bucket-owner-read',
  'bucket-owner-full-control',
];

// http://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html

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
  constructor(opts) {
    let {region, runner} = opts || {};
    region = region || 'us-east-1';
    this.region = region;
    if (!runner) {
      let r = new Runner();
      runner = r.run.bind(r);
    }
    this.runner = runner;
    // http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
    let s3region = region === 'us-east-1' ? 's3' : 's3-' + region;
    this.s3host = `${s3region}.amazonaws.com`;
  }

  /** Convert the result from the aws4.sign method into the
   * general form and return an object in the form:
   *   { url: '...', method: '...', headers: {key: 'value'}}
   */
  async __serializeRequest(req) {
    let serialized = {
      url: `${req.protocol}//${req.hostname}${req.path}`,
      method: req.method,
      headers: req.headers,
    };

    await InterchangeFormat.validate(serialized);
    return serialized;
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
   * Generate the XML body required to set tags on an object.
   *
   * This function tags an object which is key-value pairings
   * that represent each tag.
   *
   * EXAMPLE:
   * <Tagging>
   *    <TagSet>
   *       <Tag>
   *          <Key>tag1</Key>
   *          <Value>val1</Value>
   *       </Tag>
   *       <Tag>
   *          <Key>tag2</Key>
   *          <Value>val2</Value>
   *       </Tag>
   *    </TagSet>
   * </Tagging>
   */
  __generateTagSetBody(tags) {
    let doc = new libxml.Document();

    let ctx = doc.node('Tagging');
    ctx = ctx.node('TagSet');
    for (let key in tags) {
      ctx = ctx.node('Tag');
      ctx = ctx.node('Key', key);
      ctx = ctx.parent();
      ctx = ctx.node('Value', tags[key]);
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

  // Return a list of 2-tuple's that are header key and value pairings for the
  // headers to specify as the ACL for an object
  __determinePermissionsHeaders(permissions) {
    let {acl, read, write, readAcp, writeAcp, fullControl} = permissions;
    if (acl) {
      if (typeof acl !== 'string') {
        throw new Error('Canned ACL provided is not a string');
      } else if (read || write || readAcp || writeAcp || fullControl) {
        // NOTE: My reading of the documentation suggests that you can specify
        // *either* a canned ACL *or* a specific access permissions acl
        throw new Error('If you are using a canned ACL, you may not specify further permissions');
      } else if (cannedACLs.indexOf(permissions.acl) === -1) {
        throw new Error('You are requesting a canned ACL that is not valid');
      }
      return [['x-amz-acl', permissions.acl]];
    }

    let perms = [];

    if (read) {
      if (typeof read !== 'string') {
        throw new Error('Grant Read permisson is invalid');
      }
      perms.push(['x-amz-grant-read', read]);
    }

    if (write) {
      if (typeof write !== 'string') {
        throw new Error('Grant Write permisson is invalid');
      }
      perms.push(['x-amz-grant-write', write]);
    }

    if (readAcp) {
      if (typeof readAcp !== 'string') {
        throw new Error('Grant Read Acp permisson is invalid');
      }
      perms.push(['x-amz-grant-read-acp', readAcp]);
    }

    if (writeAcp) {
      if (typeof writeAcp !== 'string') {
        throw new Error('Grant Write Acp permisson is invalid');
      }
      perms.push(['x-amz-grant-write-acp', writeAcp]);
    }

    if (fullControl) {
      if (typeof fullControl !== 'string') {
        throw new Error('Grant Full Control permisson is invalid');
      }
      perms.push(['x-amz-grant-full-control', fullControl]);
    }

    return perms;
  }
 
  /**
   * Initiate a Multipart upload and return the UploadIp that
   * Amazon has assigned for this multipart upload
   * http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadInitiate.html
   */
  async initiateMultipartUpload(opts) {
    let {bucket, key, sha256, size, permissions} = opts;
    validateSha256(sha256);
    if (size <= 0) {
      // Each part must have a non-zero size
      throw new Error('Objects must be more than 0 bytes');
    } else if (size > 5 * 1024 * 1024 * 1024 * 1024) {
      // The entire file must be lower than 5 TB
      throw new Error('Object must total fewer than 5 TB'); 
    }
    let unsignedRequest = {
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
    };

    // If we have permissions, set those values on the headers
    if (permissions) {
      let permHeaders = this.__determinePermissionsHeaders(permissions);
      for (let tuple of permHeaders) {
        unsignedRequest.headers[tuple[0]] = tuple[1];
      }
    }

    let signedRequest = aws4.sign(unsignedRequest);

    let response = await this.runner({
      req: await this.__serializeRequest(signedRequest)
    });
    let uploadId = this.__getUploadId(parseS3Response(response.body), bucket, key);
    if (response.statusCode !== 200) {
      throw new Error('Expected HTTP Status Code 200, got: ' + response.statusCode);
    }
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
    if (parts.length < 1 || parts.length > 10000) {
      throw new Error('Must have between 1 and 10000 parts');
    }
    for (let num = 1 ; num <= parts.length ; num++) {
      let part = parts[num - 1];

      validateSha256(part.sha256);
      if (part.size < 5 * 1024 * 1024 && num < parts.length) {
        throw new Error(`Part ${num}/${parts.length} must be more than 5MB`);
      } else if (part.size > 5 * 1024 * 1024 * 1024) {
        throw new Error(`Part ${num} exceeds 5GB limit`);
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

      requests.push(await this.__serializeRequest(signedRequest));
    }

    return requests;
  }

  /**
   * This method is used to tag resources, and only after completion.  This is
   * done to maintain parity with single part uploads and should *not* be
   * relied upon to be atomic.  This is designed to be used in things like the
   * cost explorer.  It is not intended to be part of the public api of this
   * library, hence the name.  If it were possible to tag the multipart upload
   * at object creation as it is with single part uploads, we'd do that instead
   *
   * http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUTtagging.html
   */
  async __tagObject(opts) {
    let {bucket, key, tags} = opts || {};
    let requestBody = this.__generateTagSetBody(tags);
    // Oddly, the S3 documentation says that Content-MD5 will be a required
    // header but not in the table that I've come to expect from EC2.  Since
    // we're doing V4 request signing, this shouldn't be an issue but since the
    // docs aren't clear and MD5 is nearly free let's just do it to be safe
    let contentMD5 = crypto.createHash('md5').update(requestBody).digest('hex');
    let signedRequest = aws4.sign({
      service: 's3',
      region: this.region,
      method: 'PUT',
      protocol: 'https:',
      hostname: `${bucket}.${this.s3host}`,
      path: `/${key}?tagging=`,
      headers: {
        'content-md5': contentMD5,
      },
      body: requestBody,
    });

    let response = await this.runner({
      req: await this.__serializeRequest(signedRequest),
      body: requestBody,
    });

    parseS3Response(response.body);

    if (response.statusCode !== 200) {
      throw new Error('Expected HTTP Status Code 200, got: ' + response.statusCode);
    }
  }

  /**
   * Mark a multipart upload as completed. 
   *
   * http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadComplete.html
   */
  async completeMultipartUpload(opts) {
    let {bucket, key, uploadId, etags, tags} = opts;
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

    let response = await this.runner({
      req: await this.__serializeRequest(signedRequest),
      body: requestBody,
    });

    let multipartEtag = this.__getMultipartEtag(parseS3Response(response.body), bucket, key);

    if (response.statusCode !== 200) {
      throw new Error('Expected HTTP Status Code 200, got: ' + response.statusCode);
    }

    if (tags) {
      await this.__tagObject(opts);
    }

    return multipartEtag;
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

    let response = await this.runner({
      req: await this.__serializeRequest(signedRequest),
    });

    parseS3Response(response.body);

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
    //grant-*-acp, grant-*
    let {bucket, key, sha256, size, tags, permissions} = opts;
    !validateSha256(sha256);
    if (size <= 0 || size > 5 * 1024 * 1024 * 1024) {
      throw new Error('Objects must be more than 0 bytes and less than 5GB');
    }
    let unsignedRequest = {
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
    };

    // Set any tags.  For once, AWS Tags are atomic!
    if (tags) {
      unsignedRequest.headers['x-amz-tagging'] = qs.stringify(tags);
    }

    // If we have permissions, set those values on the headers
    if (permissions) {
      let permHeaders = this.__determinePermissionsHeaders(permissions);
      for (let tuple of permHeaders) {
        unsignedRequest.headers[tuple[0]] = tuple[1];
      }
    }

    let signedRequest = aws4.sign(unsignedRequest);

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
