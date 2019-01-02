const { Controller, Client } = require('../');
const { DigestStream } = require('../lib/digest-stream');
const BUCKET = 'test-bucket-for-any-garbage';
const uuid = require('uuid');
const fs = require('fs');
const {tmpName} = require('tmp');

const bigfile = __dirname + '/../bigfile';

function rm(f) {
  return new Promise((resolve, reject) => {
    fs.unlink(f, err => {
      if (err) {
        return reject(err);
      }
      return resolve();
    });
  });
}

if (!process.env.SKIP_REAL_S3_TESTS) {
  describe('Works with live S3', () => {
    let controller;
    let client;
    let key;
    let bigfilesize;
    let bigfilehash;
    let keys = [];
    let cleanupFiles = [];

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
      await Promise.all(cleanupFiles.map(f => rm(f)));
    });
    
    async function validateDownload() {
      let output = await new Promise((resolve, reject) => {
        tmpName((err, name) => {
          if (err) {
            return reject(err);
          }
          return resolve(name);
        });
      });
      try {
        await client.downloadObject({bucket: BUCKET, key, output});
      } catch (err) {
        await rm(output);
        throw err;
      }

      let ds = new DigestStream();
      let rs = fs.createReadStream(output);
      let ws = fs.createWriteStream('/dev/null');

      return new Promise((resolve, reject) => {
        ds.on('error', reject);
        rs.on('error', reject);
        ws.on('error', reject);

        ds.on('end', () => {
          if (ds.size !== bigfilesize) {
            reject(new Error('Downloaded file size mismatch'));
          }
          if (ds.hash !== bigfilehash) {
            reject(new Error('Downloaded file sha256 mismatch'));
          }
          resolve();
        });

        rs.pipe(ds).pipe(ws);
      });
    }

    it('should be able to upload and download a single-part file (identity encoding)', async () => {
      let upload = await client.prepareUpload({
        filename: bigfile,
        forceSP: true,
      });

      let tags = {tag1: 'tag-1-value'};
      let metadata = {metadata1: 'metadata-1-value'};
      let contentType = 'application/json';

      let request = await controller.generateSinglepartRequest({
        bucket: BUCKET,
        key,
        sha256: upload.sha256,
        size: upload.size,
        tags,
        metadata,
        contentType,
      });
      await client.runUpload(request, upload);

      await validateDownload();
    });

    it('should be able to upload and download a single-part file (gzip encoding)', async () => {
      let upload = await client.prepareUpload({
        filename: bigfile,
        forceSP: true,
        compression: 'gzip'
      });

      let tags = {tag1: 'tag-1-value'};
      let metadata = {metadata1: 'metadata-1-value'};
      let contentType = 'application/json';

      let request = await controller.generateSinglepartRequest({
        bucket: BUCKET,
        key,
        sha256: upload.sha256,
        size: upload.size,
        transferSha256: upload.transferSha256,
        transferSize: upload.transferSize,
        tags,
        metadata,
        contentType,
        contentEncoding: upload.contentEncoding,
      });
      await client.runUpload(request, upload);

      await validateDownload();
    });

    it('should be able to upload and download a multi-part file (identity encoding)', async () => {
      let upload = await client.prepareUpload({
        filename: bigfile,
        forceMP: true,
      });

      let tags = {tag1: 'tag-1-value'};
      let metadata = {metadata1: 'metadata-1-value'};
      let contentType = 'application/json';

      let uploadId = await controller.initiateMultipartUpload({
        bucket: BUCKET,
        key,
        sha256: upload.sha256,
        size: upload.size,
        metadata,
        contentType,
        contentEncoding: upload.contentEncoding,
      });

      let requests = await controller.generateMultipartRequest({
        bucket: BUCKET,
        key,
        uploadId,
        parts: upload.parts,
      });

      let result;
      try {
        result = await client.runUpload(requests, upload);
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

      await validateDownload();
    });

    it('should be able to upload and download a multi-part file (gzip encoding)', async () => {
      let upload = await client.prepareUpload({
        filename: bigfile,
        forceMP: true,
        compression: 'gzip',
      });

      let tags = {tag1: 'tag-1-value'};
      let metadata = {metadata1: 'metadata-1-value'};
      let contentType = 'application/json';

      let uploadId = await controller.initiateMultipartUpload({
        bucket: BUCKET,
        key,
        sha256: upload.sha256,
        size: upload.size,
        transferSha256: upload.transferSha256,
        transferSize: upload.transferSize,
        metadata,
        contentType,
        contentEncoding: upload.contentEncoding,
      });

      let requests = await controller.generateMultipartRequest({
        bucket: BUCKET,
        key,
        uploadId,
        parts: upload.parts,
      });

      let result;
      try {
        result = await client.runUpload(requests, upload);
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

      await validateDownload();
    });
  });
}
