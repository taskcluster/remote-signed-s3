const crypto = require('crypto');
const fs = require('fs');
const stream = require('stream');
const assume = require('assume');
//const sinon = require('sinon');

const Runner = require('../lib/runner');
const Client = require('../lib/client');

const assertReject = require('./utils').assertReject;

const runner = new Runner();

const httpbin = 'https://httpbin.org/';

const samplefile = __dirname + '/../package.json';
const samplehash = crypto.createHash('sha256').update(fs.readFileSync(samplefile)).digest('hex');
const samplesize = fs.statSync(samplefile).size;

describe('Client', () => {
  describe('Determining upload type', () => {
    let client = new Client();

    it('should pick multipart for a small file', () => {
    });
  });

  describe('Single Part Uploads', () => {
    let client;
    beforeEach(() => {
      client = new Client({
        forceSP: true,
        partsize: 250,
      });
    });

    it('should be able prepare upload', async () => {
      let info = await client.__prepareSinglepartUpload({
        forceSP: true,
        filename: samplefile,
      });
      assume(info).has.property('filename', samplefile);
      assume(info).has.property('sha256', samplehash);
      assume(info).has.property('size');
    });

    it('should run an upload', async () => {
      let info = await client.prepareUpload({
        forceSP: true,
        filename: samplefile,
      });

      let actual = await client.runUpload({
        url: httpbin + 'post',
        method: 'post', 
        headers: {sha256: samplehash}}, info);

      assume(actual.etags).deeply.equals(['NOETAG']);
      assume(actual.responses).has.lengthOf(1);
      actual = JSON.parse(actual.responses[0].body);
      assume(actual.json).deeply.equals(require('../package.json'));
      assume(actual.headers).has.property('Content-Length', Number(samplesize).toString());
      assume(actual.data).has.property('length', samplesize);
      assume(actual.headers).has.property('Sha256', samplehash);
    });
  });

  describe('Multiple Part Uploads', () => {
    let client;
    beforeEach(() => {
      client = new Client({
        forceMP: true,
      });
    });

    it('should be able prepare upload', async () => {
      let info = await client.__prepareMultipartUpload({
        forceMP: true,
        filename: samplefile,
      });
      assume(info).has.property('filename', samplefile);
      assume(info).has.property('sha256', samplehash);
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
      let info = await client.prepareUpload({
        filename: samplefile,
        forceMP: true,
      });
      console.dir(info);

      let pn = 0;
      let requests = info.parts.map(part => {
        pn++;
        return {
          url: httpbin + 'post',
          method: 'post',
          headers: {
            sha256: info.sha256,
            partSha256: part.sha256,
            partNumber: pn,
          }
        }
      });

      let actual = await client.runUpload(requests, info);

      assume(actual.etags).deeply.equals(['NOETAG']);
      assume(actual.responses).has.lengthOf(1);
      actual = JSON.parse(actual.responses[0].body);
      assume(actual.json).deeply.equals(require('../package.json'));
      assume(actual.headers).has.property('Content-Length', Number(samplesize).toString());
      assume(actual.data).has.property('length', samplesize);
      assume(actual.headers).has.property('Sha256', samplehash);
    });
  });
});
