import http from 'http';
import https from 'https';
import crypto from 'crypto';
import qs from 'querystring';

import _debug from 'debug';
import aws4 from 'aws4';
import libxml from 'libxmljs';

import Runner from './runner';
import InterchangeFormat from './interchange-format';
import {Joi, schemas, runSchema} from './schemas';

const debug = _debug('remote-s3:Bucket');

// Rather than generating for every abort invocation
const emptysha256 = crypto.createHash('sha256').update('').digest('hex');

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
    opts = runSchema(opts || {}, Joi.object().keys({
      region: Joi.string().default('us-east-1'),
      runner: Joi.any(),
      runnerOpts: Joi.object(),
    }).without('runner', 'runnerOpts').optionalKeys(['runner', 'runnerOpts']));

    this.region = opts.region;
    let runner = opts.runner;

    // we don't want to get too specific into the internal API of the run() method,
    // so we're only saving the .run() method of the runner class that we're creating here. 
    if (!opts.runner) {
      let r = new Runner(opts.runnerOpts || {});
      runner = r.run.bind(r);
    }

    this.runner = runner;
    // http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
    let s3region = this.region === 'us-east-1' ? 's3' : 's3-' + this.region;
    this.s3host = `${s3region}.amazonaws.com`;

    // These values are used for unit testing to direct the 
    this.s3protocol = 'https:';
    this.s3port = undefined;
  }

  /** Convert the result from the aws4.sign method into the
   * general form and return an object in the form:
   *   { url: '...', method: '...', headers: {key: 'value'}}
   */
  __serializeRequest(req) {
    let serialized = {
      url: `${req.protocol}//${req.hostname}${req.path}`,
      method: req.method,
      headers: req.headers,
    };

    InterchangeFormat.validate(serialized);
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
    etags = runSchema(etags, Joi.array().items(Joi.string()).min(1).max(10000).required());
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

    return doc.toString().toString();
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
    // TODO: Figure out how to double check that all keys and values
    // match a pattern or something
    tags = runSchema(tags, Joi.object().required());

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

    return doc.toString().trim();
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
    permissions = runSchema(permissions, schemas.permissions.required());
    let {acl, read, write, readAcp, writeAcp, fullControl} = permissions;
    if (acl) {
      return [['x-amz-acl', permissions.acl]];
    }

    let perms = [];

    if (read) {
      perms.push(['x-amz-grant-read', read]);
    }

    if (write) {
      perms.push(['x-amz-grant-write', write]);
    }

    if (readAcp) {
      perms.push(['x-amz-grant-read-acp', readAcp]);
    }

    if (writeAcp) {
      perms.push(['x-amz-grant-write-acp', writeAcp]);
    }

    if (fullControl) {
      perms.push(['x-amz-grant-full-control', fullControl]);
    }

    return perms;
  }

  __s3hostname(bucket) {
    let hostname = `${bucket}.${this.s3host}`;
    if (this.s3port) {
      hostname += ':' + this.s3port;
    }
    return hostname;
  }
 
  /**
   * Initiate a Multipart upload and return the UploadIp that
   * Amazon has assigned for this multipart upload
   * http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadInitiate.html
   */
  async initiateMultipartUpload(opts) {

    opts = runSchema(opts, Joi.object().keys({
      bucket: schemas.bucket.required(),
      key: schemas.key.required(),
      sha256: schemas.sha256.required(),
      size: schemas.mpSize.required(),
      permissions: schemas.permissions,
    }).optionalKeys('permissions'));

    let {bucket, key, sha256, size, permissions} = opts;

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
      protocol: this.s3protocol,
      hostname: this.__s3hostname(bucket),
      path: `/${key}?uploads=`,
      headers: {
        'x-amz-meta-content-sha256': sha256,
        'x-amz-meta-content-length': size,
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
      req: this.__serializeRequest(signedRequest)
    });
    
    let parsedResponse = parseS3Response(response.body);

    let uploadId = this.__getUploadId(parsedResponse, bucket, key);

    if (response.statusCode === 200) {
      return uploadId;
    } else {
      throw new Error('Could not initiate multipart upload');
    }
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
    opts = runSchema(opts, Joi.object().keys({
      bucket: schemas.bucket.required(),
      key: schemas.key.required(),
      uploadId: Joi.string().required(),
      parts: schemas.parts.required(),
    }));

    let {bucket, key, uploadId, parts} = opts;
    let requests = [];

    for (let num = 1 ; num <= parts.length ; num++) {
      let part = parts[num - 1];

      // This is sort of hard to encapsulate in Joi for me, so for the time
      // being we'll leave this as an external check until I can figure out how
      // to say that all parts until the last must be at least 5MB
      if (part.size < 5 * 1024 * 1024 && num < parts.length) {
        throw new Error(`Part ${num}/${parts.length} must be more than 5MB, except last`);
      }

      let signedRequest = aws4.sign({
        service: 's3',
        region: this.region,
        method: 'PUT',
        protocol: this.s3protocol,
        hostname: this.__s3hostname(bucket),
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
    opts = runSchema(opts, Joi.object().keys({
      bucket: schemas.bucket.required(),
      key: schemas.key.required(),
      tags: schemas.tags.required(),
    }));

    let {bucket, key, tags} = opts;

    let requestBody = this.__generateTagSetBody(tags);

    let signedRequest = aws4.sign({
      service: 's3',
      region: this.region,
      method: 'PUT',
      protocol: this.s3protocol,
      hostname: this.__s3hostname(bucket),
      path: `/${key}?tagging=`,
      body: requestBody,
    });

    let response = await this.runner({
      req: this.__serializeRequest(signedRequest),
      body: requestBody,
    });


    parseS3Response(response.body);

    if (response.statusCode !== 200) {
      throw new Error('Could not tag object');
    }
  }

  /**
   * Mark a multipart upload as completed. 
   *
   * http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadComplete.html
   */
  async completeMultipartUpload(opts) {
    opts = runSchema(opts, Joi.object({
      bucket: schemas.bucket.required(),
      key: schemas.key.required(),
      etags: schemas.etags.required(),
      tags: schemas.tags,
      uploadId: schemas.uploadId.required(),
    }).optionalKeys('tags'));

    let {bucket, key, uploadId, etags, tags} = opts;

    // I'm not sure why, but for some reason the AWS4 library generates the
    // incorrect SHA256 for *this* and only *this* body.  I have to calculate
    // the body sha256 myself, or else what happens is that the requestBody
    // string is hashed with its value and an extra newline character, but then
    // the data written does not have that newline, and never did.  I suspect
    // that there's something broken in the aws4 library here
    let requestBody = this.__generateCompleteUploadBody(etags);
    let requestBodySha256 = crypto.createHash('sha256').update(requestBody);

    let signedRequest = aws4.sign({
      service: 's3',
      region: this.region,
      method: 'POST',
      protocol: this.s3protocol,
      hostname: this.__s3hostname(bucket),
      path: `/${key}?uploadId=${uploadId}`,
      headers: {
        'X-Amz-Content-Sha256': requestBodySha256.digest('hex'),
        'Content-Length': requestBody.length,
      },
    });

    let response = await this.runner({
      req: this.__serializeRequest(signedRequest),
      body: requestBody,
    });

    let parsedResponse = parseS3Response(response.body);

    let multipartEtag = this.__getMultipartEtag(parsedResponse, bucket, key);

    if (response.statusCode !== 200) {
      throw new Error('Could not complete a multipart upload');
    }

    if (tags) {
      await this.__tagObject(opts);
    }

    return multipartEtag;
  }

  // http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadAbort.html
  async abortMultipartUpload(opts) {
   opts = runSchema(opts, Joi.object({
     bucket: schemas.bucket.required(),
     key: schemas.key.required(),
     uploadId: schemas.uploadId.required(),
   }));

    let {bucket, key, uploadId} = opts;
    let signedRequest = aws4.sign({
      service: 's3',
      region: this.region,
      method: 'DELETE',
      protocol: this.s3protocol,
      hostname: this.__s3hostname(bucket),
      headers: {
        'content-length': 0,
        'x-amx-content-sha256': emptysha256,
      },
      path: `/${key}?uploadId=`,
    });

    let response = await this.runner({
      req: this.__serializeRequest(signedRequest),
    });

    parseS3Response(response.body);

    if (response.statusCode !== 204) {
      throw new Error('Could not abort multipart upload');
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
    opts = runSchema(opts, Joi.object().keys({
      bucket: schemas.bucket.required(),
      key: schemas.key.required(),
      sha256: schemas.sha256.required(),
      size: schemas.spSize.required(),
      tags: schemas.tags,
      permissions: schemas.permissions,
    }).optionalKeys('tags', 'permissions'));

    let {bucket, key, sha256, size, tags, permissions} = opts;

    let unsignedRequest = {
      service: 's3',
      region: this.region,
      method: 'PUT',
      protocol: this.s3protocol,
      hostname: this.__s3hostname(bucket),
      path: `/${key}`,
      headers: {
        'content-length': size,
        'x-amz-content-sha256': sha256,
        'x-amz-meta-content-sha256': sha256,
        'x-amz-meta-content-length': size,
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
  if (!body || body.length === 0) {
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

// Let's export these
Controller.schemas = schemas;

module.exports = {
  Controller,
  parseS3Response
};
