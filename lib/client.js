'use strict';

const assert = require('assert');
const crypto = require('crypto');
const zlib = require('zlib');
const stream = require('stream');

const fs = require('mz/fs');
const {tmpName} = require('tmp');

const { Runner } = require('./runner');
const InterchangeFormat = require('./interchange-format');
const { DigestStream } = require('./digest-stream');
const { Joi, schemas, runSchema, MB, GB, TB } = require('./schemas');

const MAX_S3_CHUNKS = 10000;

/**
 * This class represents a client of a service implemented using the
 * `Controller` class.
 */
class Client {

  constructor(opts) {
    opts = runSchema(opts || {}, Joi.object().keys({
      runner: Joi.any(),
      runnerOpts: Joi.object().default({}),
      partsize: Joi.number().min(5*MB).max(5*GB).default(25*MB),
      multisize: Joi.number().default(100*MB),
    }).without('runner', 'runnerOpts')
      .optionalKeys(['runner', 'runnerOpts']));

    let {runner, runnerOpts, partsize, multisize} = opts;

    // Store value and not reference
    this.partsize = partsize+0;

    // Unlike the Controller, which has much simplier usage, here we do want
    // the full Runner api to be available to the client
    if (!runner) {
      runner = new Runner(opts.runnerOpts);
    }
    
    // The Runner to use for this
    this.runner = runner;

    // The minimum size before switching to multipart
    this.multisize = opts.multisize;
  }


  async __prepareSinglepartUpload(opts) {
    let {filename} = opts;
    let filestats = await fs.stat(filename);
    let sha256 = crypto.createHash('sha256');
    let size = 0;
    let stream = fs.createReadStream(filename, {start: 0});
    return new Promise((resolve, reject) => {
      stream.on('error', reject);

      stream.on('data', data => {
        sha256.update(data);
        size += data.length;
      });

      stream.on('end', async () => {
        let finishedstats = await fs.stat(filename);
        if (size !== filestats.size) {
          throw new Error('File has a different number of bytes than was hashed');
        } else if (finishedstats.size !== filestats.size) {
          reject(new Error('File changed size during preperation'));
        } else if (finishedstats.mtime.getTime() !== filestats.mtime.getTime()){
          reject(new Error('File was modified during preperation'));
        } else if (finishedstats.ino !== filestats.ino){
          reject(new Error('File has changed inodes'));
        } else {
          sha256 = sha256.digest('hex');
          resolve({
            filename,
            sha256,
            size,
          });
        }
      });
    });
  }

  async __prepareMultipartUpload(opts) {
    opts = opts || {};
    let {filename, partsize} = opts;
    // Ensure we're copying the value and not changing it
    partsize = (partsize || this.partsize);

    let sha256 = crypto.createHash('sha256');
    let filestats = await fs.stat(filename);
    let size = 0; // the computed size, to check against result of stat();
    let partcount = Math.ceil(filestats.size / partsize);
    let parts = [];

    if (partsize/size < 2) {
      throw new Error('Multipart upload must have at least 2 parts');
    }

    for (let part = 0 ; part < partcount ; part++) {
      await new Promise((resolve, reject) => {
        let parthash = crypto.createHash('sha256');
        let start = part * partsize;
        let end = start + partsize - 1;
        let currentPartsize = 0;

        let partstream = fs.createReadStream(filename, {start, end});

        partstream.on('error', reject);

        partstream.on('data', data => {
          size += data.length;
          currentPartsize += data.length;
          sha256.update(data);
          parthash.update(data);
        });

        partstream.on('end', () => {
          // All parts other than the last one should have a size no greater than
          // the partsize requested
          if (part < partcount - 1) {
            if (partsize !== currentPartsize) {
              throw new Error('All parts before last part must be exactly requested size');
            }
          } else if (part === partcount - 1) {
            if (currentPartsize > partsize) {
              throw new Error('Final part exceeds allowed size');
            }
          }
          parts.push({sha256: parthash.digest('hex'), size: currentPartsize, start});
          resolve();
        });
      });
    }

    sha256 = sha256.digest('hex');

    // Now make sure that in the meantime that the file didn't change out from
    // under us.  It's still possible for a properly motivated person
    // to reset the mtime and size to what we expect, but these checks
    // are more about non-intentional mistakes.  We cannot compare hashes before
    // and after since we're computing the hash 
    let finishedstats = await fs.stat(filename);
    if (size !== filestats.size) {
      throw new Error('File has a different number of bytes than was hashed');
    } else if (finishedstats.size !== filestats.size) {
      reject(new Error('File changed size during preperation'));
    } else if (finishedstats.mtime.getTime() !== filestats.mtime.getTime()){
      reject(new Error('File was modified during preperation'));
    } else if (finishedstats.ino !== filestats.ino){
      reject(new Error('File has changed inodes'));
    } else {
      return {filename, sha256, size, parts};
    }
  }

  __useMulti(size, forceMP, forceSP) {
    // We want the ability to force multi or single
    forceMP = process.env.FORCE_MP || forceMP;
    forceSP = process.env.FORCE_SP || forceSP;

    if (!size) {
      throw new Error('You must provide a size');
    }

    if (forceMP && forceSP) {
      throw new Error('Forcing singlepart and multipart is mutually exclusive');
    } else if (forceMP) {
      return true;
    } else if (forceSP) {
      return false;
    } else if (size >= this.multisize) {
      return true;
    } else {
      return false;
    }
  }

  async prepareUpload(opts) {
    opts = runSchema(opts, Joi.object().keys({
      filename: Joi.string().required(),
      forceMP: Joi.boolean().truthy(),
      forceSP: Joi.boolean().truthy(),
      partsize: Joi.number().max(5 * GB).default(this.partsize),
      compression: Joi.string().valid(['identity', 'gzip']).default('identity'),
      compressionScratchFile: Joi.string()
    }).without('forceSP', 'forceMP'));

    let {
      filename,
      partsize,
      forceMP, 
      forceSP,
      compression,
      compressionScratchFile
    } = opts;

    let _filename;
    let compressionInfo;

    if (compression === 'identity') {
      _filename = filename;
    } else {
      if (!compressionScratchFile) {
        _filename = await new Promise((resolve, reject) => {
          tmpName((err, name) => {
            if (err) return reject(err);
            resolve(name);
          });
        });
      } else {
        _filename = compressionScratchFile;
      }

      compressionInfo = await this.__compressFile({
        inputFilename: filename,
        outputFilename: _filename,
        compressor: compression,
      });

    }

    let filesize = (await fs.stat(_filename)).size;

    if (typeof filesize !== 'number') {
      throw new Error('Unable to determine filesize of ' + filename);
    }

    let result;
    if (this.__useMulti(filesize, forceMP, forceSP)) {
      result = await this.__prepareMultipartUpload({filename: _filename, partsize});
    } else {
      result = await this.__prepareSinglepartUpload({filename: _filename});
    }

    // NOTE: This will overwrite the sha256 and size values from the __prepare*
    // functions.  This behaviour is desired so that we can read the files in
    // the minmal number of times.  We only want to read the original file the
    // single time.  If we're doing compression, this should be when we feed it
    // to the GZIP encoder.  If we're not doing compression, we can use the raw
    // values we're getting from __prepare* functions as they're derived from
    // the actual file
    if (compressionInfo) {
      Object.assign(result, compressionInfo);
    } else {
      result.transferSha256 = result.sha256;
      result.transferSize = result.size;
    }

    return result;
  }

  /**
   * Compress a file and return the post compression SHA256 and size.
   */
  async __compressFile(opts) {
    opts = runSchema(opts, Joi.object().keys({
      inputFilename: Joi.string().required(),
      compressor: Joi.string().valid(['gzip']).default('gzip'),
      outputFilename: Joi.string(),
    }));

    let {inputFilename, compressor, outputFilename, sha256, size} = opts;

    let inputStream = fs.createReadStream(inputFilename);
    let outputStream = fs.createWriteStream(outputFilename);
    let preCompressionDigest = new DigestStream();
    let postCompressionDigest = new DigestStream();
    let compressionStream;
    let contentEncoding;
    switch (compressor) {
      case 'gzip':
        compressionStream = zlib.createGzip();
        contentEncoding = 'gzip';
        break;
    }
    return new Promise((resolve, reject) => {
      inputStream.on('error', reject);
      outputStream.on('error', reject);
      preCompressionDigest.on('error', reject);
      postCompressionDigest.on('error', reject);
      compressionStream.on('error', reject);

      outputStream.on('finish', () => {
        resolve({
          sha256: preCompressionDigest.hash,
          size: preCompressionDigest.size,
          transferSha256: postCompressionDigest.hash,
          transferSize: postCompressionDigest.size,
          contentEncoding,
          filename: outputFilename,
        });
      });

      inputStream
        .pipe(preCompressionDigest)
        .pipe(compressionStream)
        .pipe(postCompressionDigest)
        .pipe(outputStream);
    });
  }

  /**
   * Take a request and upload metadata and return a string wihch represents
   * an invocation of the curl command line which approximates the equivalent
   * http requests which this class would make
   */
  __curl(request, upload) {
    let {headers, method, url} = request;
    let {filename} = upload;
    let command = ['curl'];
    method = method || 'GET';
    command.push(`-X ${method}`);
    for (let header in headers) {
      command.push(`-H "${header}: ${headers[header]}"`);
    }
    command.push(url);
    command.push(`--data-binary @${upload.filename}`)
    return command.join(' ');
  }

  /**
   * Take the list of requests in interchange format and 
   * run them using the information from the upload preperation
   * list
   */
  async runUpload(request, upload) {
    upload = runSchema(upload, Joi.object().keys({
      filename: Joi.string().required(),
      sha256: schemas.sha256.required(),
      size: Joi.number().required(),
      transferSha256: schemas.sha256.required(),
      transferSize: Joi.number().required(),
      // NOTE: the contentEncoding parameter isn't used, but is allowed so that
      // we can pass the object we received from prepareUpload into this
      // function verbatim
      contentEncoding: Joi.string(),
      parts: schemas.parts.required(),
    }).optionalKeys('parts'));

    let {filename, sha256, size, transferSha256, transferSize, parts} = upload;
    let etags = [];
    let responses = [];

    // If we're not doing content-encoding, we're going to just use the
    // content-sha256 for the upload.  If we are doing content-encoding, we're
    // going to need to replace the sha256 and size with the transfer* version
    // of both
    let _sha256 = sha256;
    let _size = size;
    if (transferSha256 && transferSize) {
      _sha256 = transferSha256;
      _size = transferSize;
    } else if (transferSha256 || transferSize) {
      throw new Error('When using transferSha256, transferSize is mandatory');
    }

    // For single part uploads, we get a single request and a single upload
    // object.  We're going to change those into being lists so that we don't
    // need to have a different method signature compared to the multipart
    // uploads
    if (!parts) {
      parts = [{sha256: _sha256, size: _size, start: 0}];
    }
    if (!Array.isArray(request)) {
      request = [request];
    }

    // Validate that all request we're about to run are in the correct format
    for (let req of request) {
      await InterchangeFormat.validate(req);
    }

    // If we have a differing number of requests and upload parts, we've gotten
    // invalid inputs and should throw an Error
    if (request.length !== parts.length) {
      throw new Error('Number of requests does not match number of upload parts');
    }

    // Now we actually run the requests.  Here's where we'd implement concurrency
    // if we wanted it
    for (let n = 0; n < request.length ; n++) {
      let {sha256, start, size} = parts[n];
      let req = request[n];

      // We create a body factory because we want to use streaming while being
      // able to do retries.  This is better than doing fully buffered requests
      function body() {
        let end = start + size - 1;
        return fs.createReadStream(filename, {start, end});
      };

      let result = await this.runner.run({req, body});

      if (result.statusCode >= 300) {
        let err = new Error(`Failed to run a request ${req.method} ${req.url}`);
        err.url = req.url;
        err.method = req.method;
        err.headers = req.headers;
        err.body = result.body.toString();
        throw err;
      }

      let etag;

      if (result && result.headers && result.headers.etag) {
        // This header is occasionally returned wrapped in quotation marks, but
        // the S3 api seems to be able to understand the string when it has
        // them, so we're going to pass it back verbatim, aside from .trim()
        etag = result.headers.etag.trim();
      }

      // I'm not entirely happy with this.  It's almost completely here for
      // unit tests.  It's not dangerous because a lack of an ETag, or the
      // incorrect value, will just cause the upload to fail for a multipart
      // upload because it cannot commit the upload and it is not important for
      // a single part upload
      etags.push(etag || 'NOETAG');
      responses.push(result);
    }
    return {etags, responses};
  }

  /**
   * Run a request without verification.  This is intended for small 'convenience'
   * calls, like doing redirects.  The response body is stored in memory and the
   * results of a runner.run() call are returned as a promise.  The fields of the
   * resolution value are {body, headers, statusCode, statusMessage}
   */
  async runUnverifiedRequest({request, body}) {
    return this.runner.run({req: request, body: body, streamingOutput: false});
  }

  /**
   * Download a file and save it to the location specified by 'output'.  Does
   * validation of download file, and in the case of content-encoding
   * resources, will also decode the file and validate the decoded version is
   * valid and matches expectations.  The output option is either a string
   * which is a filename to store the output, or a stream factory.  We do not
   * accept streams directly because they do not support retries a stream
   * factory is a function which takes no arguments and returns a Writable
   * Stream
   */
  async runDownload({request, output}) {
    await InterchangeFormat.validate(request);

    let preDecompressionDigest = new DigestStream();
    let postDecompressionDigest = new DigestStream();
    let outputStream;
    if (typeof output === 'string') {
      outputStream = fs.createWriteStream(output);
    } else if (typeof output === 'function') {
      outputStream = output();
    } else {
      throw new Error('Output is not a supported format');
    }
    let decompressionStream;

    return new Promise(async (resolve, reject) => {
      preDecompressionDigest.on('error', reject);
      postDecompressionDigest.on('error', reject);
      outputStream.on('error', reject);
      
      let result = await this.runner.run({req: request, streamingOutput: true});

      let contentEncoding = result.headers['content-encoding'];
      let contentLength = result.headers['content-length'];
      let expectedTransferSha256 = result.headers['x-amz-meta-transfer-sha256'];
      let expectedSha256 = result.headers['x-amz-meta-content-sha256'];
      let expectedTransferSize = result.headers['x-amz-meta-transfer-length'];
      let expectedSize = result.headers['x-amz-meta-content-length'];

      // We're going to collect these and throw a single error in hopes that it
      // saves developers time having to redo all their testing for each error
      // they find
      let headerErrors = [];

      if (contentEncoding && contentEncoding !== 'gzip' && contentEncoding !== 'identity') {
        headerErrors.push('Content-Encoding is specified with invalid value');
      }

      if (!contentLength) {
        headerErrors.push('Content-Length is mandatory but absent');
      }
      if (!expectedTransferSha256) {
        headerErrors.push('Transfer-Sha256 is mandatory but absent');
      }
      if (!expectedSha256) {
        headerErrors.push('Content-Sha256 is mandatory but absent');
      }
      if (!expectedTransferSize) {
        headerErrors.push('Transfer-Size is mandatory but absent');
      }
      if (!expectedSize) {
        headerErrors.push('Content-Size is mandatory but absent');
      }

      try {
        contentLength = parseInt(contentLength, 10);
      } catch (err) {
        headerErrors.push('Content-Length is not an integer');
      }

      try {
        expectedTransferSize = parseInt(expectedTransferSize, 10);
      } catch (err) {
        headerErrors.push('Transfer-Size is not an integer');
      }

      try {
        expectedSize = parseInt(expectedSize, 10);
      } catch (err) {
        headerErrors.push('Content-Size is not an integer');
      }

      if (headerErrors.length > 0) {
        let err = new Error('Errors in the header values: ' + headerErrors.join(', '));
        err.errors = headerErrors;
        err.headers = result.headers;
        return reject(err);
      }

      if (contentEncoding && contentEncoding === 'gzip') {
        decompressionStream = new zlib.createGunzip();
      } else {
        decompressionStream = new stream.PassThrough();
      }

      decompressionStream.on('error', reject);
      result.bodyStream.on('error', reject);

      outputStream.on('finish', () => {
        let bodyErrors = [];
        if (preDecompressionDigest.hash !== expectedTransferSha256) {
          bodyErrors.push('Transfer Sha256 mismatch');
        }
        if (postDecompressionDigest.hash !== expectedSha256) {
          bodyErrors.push('Content Sha256 mismatch');
        }
        if (preDecompressionDigest.size !== expectedTransferSize) {
          bodyErrors.push('Transfer Size mismatch');
        }
        if (postDecompressionDigest.size !== expectedSize) {
          bodyErrors.push('Content Size mismatch');
        }
        if (preDecompressionDigest.size !== contentLength) {
          bodyErrors.push('Content-Length header and Transfer-Size do not match');
        }
        if (bodyErrors.length > 0) {
          let err = new Error('Errors in the body: ' + bodyErrors.join(', '));
          err.errors = bodyErrors;
          reject(err);
        } else {
          return resolve();
        }
        reject(new Error('unimplemented'));
      });

      result.bodyStream
        .pipe(preDecompressionDigest)
        .pipe(decompressionStream)
        .pipe(postDecompressionDigest)
        .pipe(outputStream);

    });
  }


  async downloadUrl({url, output}) {
    return this.runDownload({
      request: {
        url,
        method: 'GET',
        headers: {},
      },
      output,
    });
  }

  async downloadObject({region = 'us-east-1', bucket, key, output}) {
    region = region === 'us-east-1' ? 's3' : 's3-' + region;
    let url = `https://${bucket}.${region}.amazonaws.com/${key}`;
    return this.downloadUrl({url, output});
  }


}

module.exports = {
  Client
};
