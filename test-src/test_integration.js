const { Controller, Client } = require('../');
const DigestStream = require('../lib/digest-stream');
const BUCKET = 'test-bucket-for-any-garbage';
const uuid = require('uuid');
const fs = require('fs');

const bigfile = __dirname + '/../bigfile';

if (!process.env.SKIP_REAL_S3_TESTS) {
  describe('Works with live S3', () => {
    let controller;
    let client;
    let key;
    let bigfilesize;
    let bigfilehash;
    let keys = [];

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
    });

    afterEach(() => {
      keys.push(key);
    });

    after(async () => {
      for (let key of keys) {
        try {
          await controller.deleteObject({bucket: BUCKET, key});
        } catch (err) {
          console.log(`WARNING: failed to cleanup ${BUCKET}/${key}`);
        }
      }
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
      await client.runUpload(request, {filename, sha256, size});

    });

    it('should be able to upload a single-part file (gzip encoding)', async () => {
      const outfile = bigfile + '.gz';

      let {filename, sha256, size} = await client.prepareUpload({
        filename: bigfile,
        forceSP: true,
      });

      let {transferSha256, transferSize, contentEncoding} = await client.compressFile({
        inputFilename: bigfile,
        outputFilename: outfile,
        compressor: 'gzip',
        sha256,
        size,
      });

      let tags = {tag1: 'tag-1-value'};
      let metadata = {metadata1: 'metadata-1-value'};
      let contentType = 'application/json';

      let request = await controller.generateSinglepartRequest({
        bucket: BUCKET,
        key,
        sha256,
        size,
        transferSha256,
        transferSize,
        tags,
        metadata,
        contentType,
        contentEncoding: 'gzip',
      });
      await client.runUpload(request, {filename: outfile, sha256: transferSha256, size: transferSize});
    });

    it('should be able to upload a multi-part file (identity encoding)', async () => {
      let {filename, sha256, size, parts} = await client.prepareUpload({
        filename: bigfile,
        forceMP: true,
      });

      let tags = {tag1: 'tag-1-value'};
      let metadata = {metadata1: 'metadata-1-value'};
      let contentType = 'application/json';

      let uploadId = await controller.initiateMultipartUpload({
        bucket: BUCKET,
        key,
        sha256,
        size,
        metadata,
        contentType,
      });

      let requests = await controller.generateMultipartRequest({
        bucket: BUCKET,
        key,
        uploadId,
        parts,
      });

      let result;
      try {
        result = await client.runUpload(requests, {filename, sha256, size, parts});
      } catch (err) {
        await controller.abortMultipartUpload({bucket: BUCKET, key, uploadId});
        throw err;
      }

      let finalEtag = await controller.completeMultipartUpload({
        bucket: BUCKET,
        key,
        etags: result.etags,
        tags,
        uploadId,
      });

    });

    it('should be able to upload a multi-part file (gzip encoding)', async () => {
      const outfile = bigfile + '.gz';
      let {sha256, size} = await client.prepareUpload({
        filename: bigfile,
        forceMP: true,
      });

      let {transferSha256, transferSize, contentEncoding} = await client.compressFile({
        inputFilename: bigfile,
        outputFilename: outfile,
        compressor: 'gzip',
        sha256,
        size,
      });

      let {parts} = await client.prepareUpload({
        filename: outfile,
        forceMP: true,
      });

      let tags = {tag1: 'tag-1-value'};
      let metadata = {metadata1: 'metadata-1-value'};
      let contentType = 'application/json';

      let uploadId = await controller.initiateMultipartUpload({
        bucket: BUCKET,
        key,
        sha256,
        size,
        transferSha256,
        transferSize,
        metadata,
        contentType,
        contentEncoding,
      });

      let requests = await controller.generateMultipartRequest({
        bucket: BUCKET,
        key,
        uploadId,
        parts,
      });

      let result;
      try {
        result = await client.runUpload(requests, {filename: outfile, sha256: transferSha256, size: transferSize, parts});
      } catch (err) {
        await controller.abortMultipartUpload({bucket: BUCKET, key, uploadId});
        throw err;
      }

      let finalEtag = await controller.completeMultipartUpload({
        bucket: BUCKET,
        key,
        etags: result.etags,
        tags,
        uploadId,
      });

    });
  });
}
