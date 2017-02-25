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
    let {runner, maxConcurrency, minMPSize, chunksize} = opts;
    // The maximum number of concurrent requests this Client
    // should run
    this.maxConcurrency = maxConcurrency;

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
    let stream = fs.createReadStream(filename);
    return new Promise((resolve, reject) => {
      stream.on('error', reject);

      stream.on('data', data => {
        sha256.update(data);
        size += data.length;
      });

      stream.on('end', async () => {
        let finishedstats = await fs.stat(filename);
        if (finishedstats.size !== filestats.size) {
          reject(new Error('File changed size during preperation'));
        } else if (finishedstats.mtime.getTime() !== filestats.mtime.getTime()){
          reject(new Error('File was modified during preperation'));
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
      throw new Error('lala');
    } else {
      return this.__prepareSinglepartUpload({filename: filename});
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

    assert(request.length === parts.length);

    for (let n = 0; n < request.length ; n++) {
      let {sha256, start, size} = parts[n];
      let req = request[n];
      function body() {
        return fs.createReadStream(filename, {start, end: start + size});
      };

      let result = await this.runner.run({req, body});
      etags.push(result.headers.etag || 'NOETAG');
      responses.push(result);
    }
    return {etags, responses};
  }

  // Take a filename and determine the information about the file
  // stored there.  This includes the overall SHA256 sum and if requested the 
  // SHA256 of each chunk of the file
  //
  // The assumption is that this file is not modified during the execution of
  // this file
  async  fileInformation(options) {
    assert(typeof options.chunksize === 'number');
    assert(typeof options.filename === 'string');
    // The requested chunksize
    let chunksize = options.chunksize;
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

    let chunks = Math.ceil(size / chunksize);
    assert(chunks <= MAX_S3_CHUNKS, 'Too many chunks, try larger chunksize');

    for (let chunk = 0 ; chunk < chunks ; chunk++) {
      await new Promise((resolve, reject) => {
        let chunkSha256 = crypto.createHash('sha256');
        let cSize = 0;

        let chunkStart = chunk * chunksize;
        let chunkEnd = chunkStart + (chunksize - 1);

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

}

module.exports = Client;
