import stream from 'stream';
import crypto from 'crypto';

// https://nodejs.org/api/stream.html#stream_implementing_a_duplex_stream
class DigestStream extends stream.Transform {
  constructor(options) {
    super(options);
    options = options || {};
    this._algorithm = options.algorithm || 'sha256';
    this._format = options.format || 'hex';
    this._hash = crypto.createHash(this._algorithm);
    this._size = 0;
  }

  _transform(chunk, encoding, callback) {
    this._hash.update(chunk);
    this._size += chunk.length;
    callback(null, chunk, encoding);
  }

  _flush(callback) {
    this.hash = this._hash.digest(this._format);
    this.size = this._size;
    callback();
  }
}

module.exports = DigestStream;
