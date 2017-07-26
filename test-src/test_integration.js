const { Controller, Client } = require('../');
const DigestStream = require('../lib/digest-stream');
const BUCKET = 'test-bucket-for-any-garbage';
const uuid = require('uuid');
const fs = require('fs');

const bigfile = __dirname + '/../bigfile';

if (!process.env.SKIP_REAL_S3_TESTS) {
  describe.only('Works with live S3', () => {
    let controller;
    let client;
    let key;
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

    beforeEach(() => {
      controller = new Controller({
        region: 'us-west-2',
      });
      client = new Client();
      key = uuid.v4();
      console.log('testing with key: ' + key);
    });

    it('should be able to upload a single-part file (identity encoding)', async () => {
      let {filename, sha256, size} = await client.prepareUpload({
        filename: bigfile,
        forceSP: true,
      });

      let tags = {tag1: 'tag-1-value'};
      let metadata = {metadata1: 'metadata-1-value'};
      let contentType = 'application/json';

      let request = await controller.generateSinglepartRequest({
        bucket: BUCKET,
        key,
        sha256,
        size,
        tags,
        metadata,
        contentType,
      });
      console.log('CURL VERSION');
      console.log(client.__curl(request, {filename, sha256, size}));
      await client.runUpload(request, {filename, sha256, size});

    });

    it('should be able to upload a single-part file (gzip encoding)', async () => {
      const outfile = bigfile + '.gz';
      let {sha256, size, transferSha256, transferSize, contentEncoding} = await client.compressFile({
        inputFilename: bigfile,
        compressor: 'gzip',
        outputFilename: outfile,
      });
      console.log({sha256, size, transferSha256, transferSize, contentEncoding});

      let tags = {tag1: 'tag-1-value'};
      let metadata = {metadata1: 'metadata-1-value'};
      let contentType = 'application/json';

      let request = await controller.generateSinglepartRequest({
        bucket: BUCKET,
        key,
        sha256: transferSha256,
        transferSha256,
        size: transferSize,
        transferSize,
        tags,
        metadata,
        contentType,
        contentEncoding: 'identity',
      });
      console.log('CURL VERSION');
      console.log(client.__curl(request, {filename: outfile, sha256, size}));
      try {
        await client.runUpload(request, {filename: outfile, sha256, size});
      } catch (err) {
        console.dir(err);
        throw err;
      }
    });
  });
}
