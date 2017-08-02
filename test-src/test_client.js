const crypto = require('crypto');
const fs = require('mz/fs');
const stream = require('stream');
const assume = require('assume');
//const sinon = require('sinon');
const http = require('http');

const DigestStream = require('../lib/digest-stream');
const Client = require('../lib/client');

const assertReject = require('./utils').assertReject;

const bigfile = __dirname + '/../bigfile';

describe('Client', () => {
  let client;
  let server;

  let bigfilesize;
  let bigfilehash;

  before(done => {
    let ds = new DigestStream();
    let rs = fs.createReadStream(bigfile);
    let ws = fs.createWriteStream('/dev/null');

    ds.on('error', done);
    rs.on('error', done);
    ws.on('error', done);

    rs.pipe(ds).pipe(ws);

    ds.on('end', () => {
      try {
        bigfilesize = ds.size;
        bigfilehash = ds.hash;
        done();
      } catch (err) {
        done(err);
      }
    });
  });

  beforeEach(done => {
    client = new Client();
    if (server) {
      server.close(err => {
        server = undefined;
        done(err);
      });
    } else {
      done();
    }
  });

  after(done => {
    if (server) {
      server.close(err => {
        server = undefined;
        done(err);
      });
    } else {
      done();
    }
  });



  describe('Determining upload type', () => {
    it('should pick singlepart for a small file', () => {
      assume(client.__useMulti(1)).to.be.false();
    });

    it('should pick multipart for a big file', () => {
      assume(client.__useMulti(1024*1024*1024*1024)).to.be.true();
    });

    it('should pick multipart for a small file when forcing multipart', () => {
      assume(client.__useMulti(1024*1024*5, true, false)).to.be.true();
    });

    it('should pick singlepart for a big file when forcing singlepart', () => {
      assume(client.__useMulti(1024*1024*1024*1024, false, true)).to.be.false();
    });
  });

  describe('Single Part Uploads', () => {
    it('should be able prepare upload', async () => {
      let info = await client.__prepareSinglepartUpload({
        filename: bigfile,
      });
      assume(info).has.property('filename', bigfile);
      assume(info).has.property('sha256', bigfilehash);
      assume(info).has.property('size');
    });

    it('should run an upload', async () => {
      let port = process.env.PORT || 8080;

      await new Promise((resolve, reject) => {
        server = http.createServer();

        server.on('request', (request, response) => {
          let size = 0;
          let sha256 = crypto.createHash('sha256');

          request.on('data', data => {
            size += data.length;
            sha256.update(data);
          });

          request.on('end', () => {
            response.writeHead(200, 'OK', {
              etag: request.headers.sha256,
            });
            response.end(JSON.stringify({
              object: request.url.slice(1),
              bytes: size,
              hash: sha256.digest('hex'),
            }));
          });
        });

        server.listen(port, 'localhost', resolve);
      });


      let info = await client.prepareUpload({
        filename: bigfile,
        partsize: 5*1024*1024,
        forceSP: true,
      });

      let pn = 0;
      let requests = {
        url: `http://localhost:${port}/object`,
        method: 'post',
        headers: {
          sha256: info.sha256
        }
      };

      let actual = await client.runUpload(requests, info);

      let expectedEtags = [info.sha256];

      assume(actual.etags).deeply.equals(expectedEtags);

      let body = JSON.parse(actual.responses[0].body);

      assume(body.object).equals('object');
      assume(body.hash).equals(info.sha256);
      assume(body.bytes).equals(info.size);

    });


  });

  describe('Compress a file', () => {
    it('should compress a file correctly', async () => {
      let result = await client.compressFile({
        inputFilename: bigfile,
        compressor: 'gzip',
        outputFilename: bigfile + '.gz',
        sha256: bigfilehash,
        size: bigfilesize,
      });

      let outputSize = (await fs.stat(bigfile + '.gz')).size;
      assume(result).has.property('transferSize', outputSize);
      let outputContents = await fs.readFile(bigfile + '.gz');
      let outputHash = crypto.createHash('sha256').update(outputContents).digest('hex');
      assume(result).has.property('transferSha256', outputHash);
    });
  });

  describe('Multiple Part Uploads', () => {
    it('should be able prepare upload', async () => {
      let info = await client.__prepareMultipartUpload({
        forceMP: true,
        filename: bigfile,
      });
      assume(info).has.property('filename', bigfile);
      assume(info).has.property('sha256', bigfilehash);
      assume(info).has.property('size');
      assume(info).has.property('parts');
      assume(info.parts).to.be.instanceof(Array);
      for (let part of info.parts) {
        assume(part).has.property('sha256');
        assume(part).has.property('size');
        assume(part).has.property('start');
      }
    });

    it('should run an upload', async () => {
      let port = process.env.PORT || 8080;

      await new Promise((resolve, reject) => {
        server = http.createServer();

        server.on('request', (request, response) => {
          let size = 0;
          let sha256 = crypto.createHash('sha256');

          request.on('data', data => {
            size += data.length;
            sha256.update(data);
          });

          request.on('end', () => {
            response.writeHead(200, 'OK', {
              etag: request.headers.partsha256,
            });
            response.end(JSON.stringify({
              partnumber: Number.parseInt(request.url.slice(1)),
              bytes: size,
              hash: sha256.digest('hex'),
            }));
          });
        });

        server.listen(port, 'localhost', resolve);
      });

      // We want to allow an arbitrary file to be used as input to the test, so
      // we're going to try two different partsizes to make it more likely we
      // hit the last part being not a full sized part
      for (let partsize of [5*1024*1024, 5*1000*1000]) {

        let info = await client.prepareUpload({
          filename: bigfile,
          partsize,
          forceMP: true,
        });

        let pn = 0;
        let requests = info.parts.map(part => {
          pn++;
          return {
            url: `http://localhost:${port}/${pn}`,
            method: 'post',
            headers: {
              sha256: info.sha256,
              partSha256: part.sha256,
              partNumber: pn,
            }
          }
        });

        let actual = await client.runUpload(requests, info);

        let expectedEtags = info.parts.map(x => x.sha256);

        assume(actual.etags).deeply.equals(expectedEtags);

        for (let x = 0; x < info.parts.length ; x++) {
          let body = JSON.parse(actual.responses[x].body);
          assume(body.partnumber).equals(x+1);
          assume(body.hash).equals(info.parts[x].sha256);
          if (x < info.parts.length - 1) {
            assume(body.bytes).equals(partsize);
          } else {
            // In case the last part is smaller than a full part
            // we want to check for that value;
            let lastpartsize = info.size % (partsize) || partsize;
            assume(body.bytes).equals(lastpartsize);
          }
        }
      }
    });
  });
});
