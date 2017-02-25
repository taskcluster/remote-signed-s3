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
      //assume(client.__determineUploadType(1)).eql(Client.prototype.__prepareSinglepartUpload);
    });
  });

  describe('Single Part Uploads', () => {
    let client;
    beforeEach(() => {
      client = new Client({
        forceSP: true,
      });
    });

    it('should be able prepare upload', async () => {
      let info = await client.__prepareSinglepartUpload({
        filename: samplefile,
      });
      assume(info).has.property('filename', samplefile);
      assume(info).has.property('sha256', samplehash);
      assume(info).has.property('size');
    });

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
      assume(actual.json).deeply.equals(require('../package.json'));
      assume(actual.headers).has.property('Content-Length', Number(samplesize).toString());
      assume(actual.data).has.property('length', samplesize);
      assume(actual.headers).has.property('Sha256', samplehash);
      console.dir(actual);
    });
  });
});
