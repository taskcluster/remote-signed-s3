import fs from 'mz/fs';
import assert from 'assert';
import crypto from 'crypto';
import Runner from './runner';

const MAX_S3_CHUNKS = 10000;

/**
 * This class represents a client of a service implemented using the
 * `Controller` class.
 */
class Client {

  constructor(opts) {
    let {runner, maxConcurrency, minMPSize, partsize} = opts || {};
    // The maximum number of concurrent requests this Client
    // should run
    this.maxConcurrency = maxConcurrency;
    this.partsize = (partsize || 200 * 1024 * 1024) + 0;

    if (!runner) {
      runner = new Runner();
    }
    
    // The Runner to use for this
    this.runner = runner;

    // The minimum size before switching to multipart
    this.minMPSize = minMPSize;
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
          parts.push({sha256: parthash.digest('hex'), size: currentPartsize, start, end});
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

  __determineUploadType(size, forceMP, forceSP) {

  }

  async prepareUpload(opts) {
    let {filename, forceMP, forceSP} = opts;

    let multipart = !!forceMP;

    let filesize = await fs.stat(filename);

    if (filesize >= this.minMPSize && !forceSP) {
      multipart = true;
    }

    if (multipart) {
      return this.__prepareMultipartUpload(opts);
    } else {
      return this.__prepareSinglepartUpload(opts);
    }
  }


  /**
   * Take the list of requests in interchange format and 
   * run them using the information from the upload preperation
   * list
   */
  async runUpload(request, upload) {
    let {filename, sha256, size, parts} = upload;
    let etags = [];
    let responses = [];

    if (!Array.isArray(request)) {
      request = [request];
    }

    if (!parts) {
      parts = [{sha256, size, start: 0}];
    }

    if (request.length !== parts.length) {
      throw new Error('Number of requests does not match number of parts');
    }

    for (let n = 0; n < request.length ; n++) {
      let {sha256, start, size} = parts[n];
      let req = request[n];
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
        err.response = result.response;
      }
      // This header is occasionally returned wrapped in quotation marks.
      let etag = result.headers.strip();
      if (etag.charAt[0] === '"') {
        etag = etag.slice(1);
        if (etag.charAt[etag.length - 1] 1== '"') {

        }
      }
      etags.push(JSON.parse(result.headers.etag) || 'NOETAG');
      responses.push(result);
    }
    return {etags, responses};
  }

  /*
  // Take a filename and determine the information about the file
  // stored there.  This includes the overall SHA256 sum and if requested the 
  // SHA256 of each chunk of the file
  //
  // The assumption is that this file is not modified during the execution of
  // this file
  async  fileInformation(options) {
    assert(typeof options.partsize === 'number');
    assert(typeof options.filename === 'string');
    // The requested partsize
    let partsize = options.partsize;
    let filename = options.filename;

    // Information that we will figure out
    let filestats = await fs.stat(filename);
    let size = filestats.size;
    let beforeMtime = filestats.mtime;
    // We also want to track the size of the file in case the file is a different
    // size than we expected
    let overallSize = 0;
    let overallSha256 = crypto.createHash('sha256');
    let chunkInfo = [];

    let chunks = Math.ceil(size / partsize);
    assert(chunks <= MAX_S3_CHUNKS, 'Too many chunks, try larger partsize');

    for (let chunk = 0 ; chunk < chunks ; chunk++) {
      await new Promise((resolve, reject) => {
        let chunkSha256 = crypto.createHash('sha256');
        let cSize = 0;

        let chunkStart = chunk * partsize;
        let chunkEnd = chunkStart + (partsize - 1);

        let chunkStream = fs.createReadStream(filename, {
          start: chunkStart,
          end: chunkEnd,
        });

        chunkStream.on('error', reject);

        chunkStream.on('data', data => {
          overallSize += data.length;
          cSize += data.length;

          overallSha256.update(data);
          chunkSha256.update(data);
        });

        chunkStream.on('end', () => {
          chunkInfo.push({
            sha256: chunkSha256.digest('hex'),
            size: cSize,
            start: chunkStart,
            end: chunkStart + cSize,
          });
          cSize = 0;
          resolve();
        });
      });
    }

    filestats = await fs.stat(filename);

    // We want to make sure that the file did not changed.  We do two different
    // comparisons of size.  Once against the results of the `fs.stat()`
    // operation and once against the bytes counted.  Maybe overkill.  We also
    // ensure that the mtime hasn't changed.  This is not the strongest
    // guartunee, but before we've generated the sha256 there's no way to ensure
    // that we've done things correct
    assert(filestats.size === size, 'Filesize changed during operation (stat)');
    assert(filestats.mtime.getTime() === beforeMtime.getTime(), 'File was modified during operation');
    assert(size === overallSize, 'Filesize changed during operation (byte count)');

    return {
      filename: filename,
      sha256: overallSha256.digest('hex'),
      size: size,
      chunks: chunkInfo,
    };
  }
  */

}

module.exports = Client;
