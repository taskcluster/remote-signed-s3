const crypto = require('crypto');
const fs = require('fs');
const stream = require('stream');
const assume = require('assume');
//const sinon = require('sinon');
const http = require('http');

const Runner = require('../lib/runner');
const Client = require('../lib/client');

const assertReject = require('./utils').assertReject;

const runner = new Runner();

const httpbin = 'https://httpbin.org/';

const samplefile = __dirname + '/../package.json';
const samplehash = crypto.createHash('sha256').update(fs.readFileSync(samplefile)).digest('hex');
const samplesize = fs.statSync(samplefile).size;


const bigfile = __dirname + '/../bigfile';
// LOL MEMORY!
const bigfilecontents = fs.readFileSync(bigfile);
const bigfilehash = crypto.createHash('sha256').update(bigfilecontents).digest('hex');
const bigfilesize = bigfilecontents.length;


describe('Client', () => {
  let client;
  let server;

  beforeEach(() => {
    client = new Client();
    if (server) {
      server.close();
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
        filename: samplefile,
      });
      assume(info).has.property('filename', samplefile);
      assume(info).has.property('sha256', samplehash);
      assume(info).has.property('size');
    });

    /*
    it('should run an upload', async () => {
      let info = await client.prepareUpload({
        filename: samplefile,
      });

      let actual = await client.runUpload({
        url: httpbin + 'post',
        method: 'post', 
        headers: {sha256: samplehash}}, info);

      assume(actual.etags).deeply.equals(['NOETAG']);
      assume(actual.responses).has.lengthOf(1);
      actual = JSON.parse(actual.responses[0].body);
      assume(actual.data).deeply.equals(fs.readFileSync(samplefile).toString());
      assume(actual.headers).has.property('Content-Length', Number(samplesize).toString());
      assume(actual.data).has.property('length', samplesize);
      assume(actual.headers).has.property('Sha256', samplehash);
    }); */


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

      let partsize = 5*1024*1024;

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
    });
  });
});
