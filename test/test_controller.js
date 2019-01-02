const crypto = require('crypto');
const http = require('http');
//const https = require('https');
const urllib = require('url');
const fs = require('fs');
const sinon = require('sinon');
const qs = require('querystring');

const assume = require('assume');
const { Controller, parseS3Response } = require('../');
const { DigestStream } = require('../lib/digest-stream');
const assertReject = require('./utils').assertReject;
const InterchangeFormat = require('../lib/interchange-format');

const createMockS3Server = require('./mock_s3');

const bigfile = __dirname + '/../bigfile';

describe('Controller', () => {
  let controller;
  let server;
  let bucket = 'test-bucket';
  let key = 'test-key';
  let port = process.env.PORT || 8080;
  let sandbox = sinon.sandbox.create();

  let bigfilesize;
  let bigfilehash;

  let apitests;

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
    controller = new Controller({vhostAddressing: false});
    controller.s3host = 'localhost';
    controller.s3protocol = 'http:';
    controller.s3port = port;
    if (server) {
      server.close(err => {
        server = undefined;
        done(err);
      });
    } else {
      done();
    }
  });

  afterEach(() => {
    sandbox.restore();
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

  describe('S3', () => {

    // Here's the list of tests we want.  Note that bucket and key are
    // automagically added to params, so only include params which aren't
    // bucket or key
    // 
    // NOTE: We have to specify params as a function because these are often
    // things (e.g. sha256 hashes) which we need to calculate in before() or
    // beforeEach() sections.  If we didn't make this as a function, we'd only
    // get the value that was generated. This is because we call it() in a loop 
    let tests = [
      {
        name: 'Initiate Multipart',
        type: 'initiateMPUpload',
        func: 'initiateMultipartUpload',
        params: () => {
          return {sha256: bigfilehash, size: bigfilesize};
        }
      }, {
        name: 'Complete Multipart',
        type: 'completeMPUpload',
        func: 'completeMultipartUpload',
        params: () => {
          return {etags: ['an-etag'], uploadId: 'an-uploadId'};
        }
      }, {
        name: 'Abort Multipart',
        type: 'abortMPUpload',
        func: 'abortMultipartUpload',
        params: () => {
          return {uploadId: 'an-uploadId'};
        }
      }, {
        name: 'Tag Object',
        type: 'tagObject',
        func: '__tagObject',
        params: () => {
          return {tags: {car: 'fast', money: 'lots'}};
        }
      }, {
        name: 'Delete Object',
        type: 'deleteObject',
        func: 'deleteObject',
        params: () => {
          return {};
        }
      },

    ];

    for (let _test of tests) {
      let {name, type, params, func} = _test;
      describe(name, () => {

        // Because we call it() in a loop
        before(() => {
          params = params();
        });

        it(`should call the ${name} API Correctly`, () => {
          return new Promise(async (pass, fail) => {
            server = await createMockS3Server({
              key,
              bucket,
              requestType: type,
              port,
            });

            server.once('unittest-success', pass);
            server.once('unittest-failure', fail);

            let args = {key, bucket};
            for (let k in params) {
              args[k] = params[k];
            }
            let result = await controller[func](args);
          });
        });
        
        for (let _errorCode of [200, 403]) {
          it(`should fail with a ${_errorCode} error`, () => {
            return new Promise(async (pass, fail) => {
              server = await createMockS3Server({
                key,
                bucket,
                requestType: `generate${_errorCode}Error`,
                port,
              });

              //server.once('unittest-success', pass);
              server.once('unittest-failure', fail);

              let args = {key, bucket};
              for (let k in params) {
                args[k] = params[k];
              }
              let result;
              try {
                let result = await controller[func](args);
                fail(new Error('should have failed'));
              } catch (err) {
                assume(err.message).matches(/We encountered an internal error. Please try again/);
                pass();
              }
            });
          });
        }
      }); 
    }
  });

  describe('API Hosts', () => {
    it('should use correct host for us-east-1', () => {
      let s3 = new Controller({region: 'us-east-1', vhostAddressing: false});
      assume(s3).has.property('s3host', 's3.amazonaws.com');
    });

    it('should use correct host for us-west-1', () => {
      let s3 = new Controller({region: 'us-west-1', vhostAddressing: false});
      assume(s3).has.property('s3host', 's3-us-west-1.amazonaws.com');
    });
  });

  describe('Base URL Generation', () => {
    beforeEach(() => {
      controller.s3protocol = 'https:';
      controller.s3host = 'localhost';
      controller.s3port = 8080;
    });

    it('vhost addressing without headers or query', () => {
      controller.vhostAddressing = true;

      let expected = {
        region: 'us-east-1',
        service: 's3',
        method: 'POST',
        protocol: 'https:',
        hostname: 'bucket.localhost:8080',
        path: '/key',
        headers: {},
      };

      let actual = controller.__generateRequestBase({
        bucket: 'bucket',
        key: 'key',
        method: 'POST',
      });

      assume(actual).deeply.equals(expected);
    });
    
    it('path addressing without headers or query', () => {
      let expected = {
        region: 'us-east-1',
        service: 's3',
        method: 'POST',
        protocol: 'https:',
        hostname: 'localhost:8080',
        path: '/bucket/key',
        headers: {},
      };

      let actual = controller.__generateRequestBase({
        bucket: 'bucket',
        key: 'key',
        method: 'POST',
      });

      assume(actual).deeply.equals(expected);
    });

    it('vhost addressing without query', () => {
      controller.vhostAddressing = true;

      let expected = {
        region: 'us-east-1',
        service: 's3',
        method: 'POST',
        protocol: 'https:',
        hostname: 'bucket.localhost:8080',
        path: '/key',
        headers: {
          'content-length': '123',
        }
      };

      let actual = controller.__generateRequestBase({
        bucket: 'bucket',
        key: 'key',
        method: 'POST',
        headers: {
          'content-length': '123',
        }
      });

      assume(actual).deeply.equals(expected);
    });
    
    it('path addressing without query', () => {
      controller.vhostAddressing = false;

      let expected = {
        region: 'us-east-1',
        service: 's3',
        method: 'POST',
        protocol: 'https:',
        hostname: 'localhost:8080',
        path: '/bucket/key',
        headers: {
          'content-length': '123',
        }
      };

      let actual = controller.__generateRequestBase({
        bucket: 'bucket',
        key: 'key',
        method: 'POST',
        headers: {
          'content-length': '123',
        }
      });

      assume(actual).deeply.equals(expected);
    });
    
    it('vhost addressing with query', () => {
      controller.vhostAddressing = true;

      let expected = {
        region: 'us-east-1',
        service: 's3',
        method: 'POST',
        protocol: 'https:',
        hostname: 'bucket.localhost:8080',
        path: '/key?key1=value1&emptykey=&key2=value2',
        headers: {
          'content-length': '123',
        }
      };

      let actual = controller.__generateRequestBase({
        bucket: 'bucket',
        key: 'key',
        method: 'POST',
        query: 'key1=value1&emptykey=&key2=value2',
        headers: {
          'content-length': '123',
        }
      });

      assume(actual).deeply.equals(expected);
    });
    
    it('path addressing with query', () => {
      controller.vhostAddressing = false;

      let expected = {
        region: 'us-east-1',
        service: 's3',
        method: 'POST',
        protocol: 'https:',
        hostname: 'localhost:8080',
        path: '/bucket/key?key1=value1&emptykey=&key2=value2',
        headers: {
          'content-length': '123',
        }
      };

      let actual = controller.__generateRequestBase({
        bucket: 'bucket',
        key: 'key',
        method: 'POST',
        query: 'key1=value1&emptykey=&key2=value2',
        headers: {
          'content-length': '123',
        }
      });

      assume(actual).deeply.equals(expected);
    });
  });

  describe('API Special checks', () => {
    describe('Initiate Multipart Upload', () => {

      it('should not allow differing transferSha256 from sha256 for identity encoding', () => {
        return assertReject(controller.initiateMultipartUpload({
          bucket: 'bucket',
          key: 'key',
          uploadId: 'uploadId',
          sha256: '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeef',
          transferSha256: '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeee',
          contentEncoding: 'identity',
          size: 0,
        }));
      });


      it('should not allow differing transferSize from size for identity encoding', () => {
        return assertReject(controller.initiateMultipartUpload({
          bucket: 'bucket',
          key: 'key',
          uploadId: 'uploadId',
          sha256: '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeef',
          transferSha256: '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeee',
          contentEncoding: 'identity',
          size: 0,
          transferSize: 1,
        }));
      });

      it('should not allow differing transferSha256 from sha256 for no encoding specified', () => {
        return assertReject(controller.initiateMultipartUpload({
          bucket: 'bucket',
          key: 'key',
          uploadId: 'uploadId',
          sha256: '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeef',
          transferSha256: '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeee',
          size: 0,
        }));
      });

      it('should not allow non-identity content coding without transferSha256', () => {
        return assertReject(controller.initiateMultipartUpload({
          bucket: 'bucket',
          key: 'key',
          uploadId: 'uploadId',
          sha256: '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeef',
          contentEncoding: 'gzip',
          size: 0,
        }));
      });
      
      it('should not allow non-identity content coding without transferSize', () => {
        return assertReject(controller.initiateMultipartUpload({
          bucket: 'bucket',
          key: 'key',
          uploadId: 'uploadId',
          sha256: '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeef',
          transferSha256: '405056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeef',
          contentEncoding: 'gzip',
          size: 0,
        }));
      });

      it('should not allow parts with size <= 0', () => {
        return assertReject(controller.initiateMultipartUpload({
          bucket: 'bucket',
          key: 'key',
          uploadId: 'uploadId',
          sha256: '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeef',
          size: 0,
        }));
      });

      it('should not allow parts with an empty string sha256', () => {
        return assertReject(controller.initiateMultipartUpload({
          bucket: 'bucket',
          key: 'key',
          uploadId: 'uploadId',
          sha256: crypto.createHash('sha256').update('').digest('hex'),
          size: 1,
        }));
      });

      it('should not allow parts with invalid sha256', () => {
        return assertReject(controller.initiateMultipartUpload({
          bucket: 'bucket',
          key: 'key',
          uploadId: 'uploadId',
          sha256: 'asdflasdf',
          size: 1,
        }));
      });

      it('should throw for invalid storage classes', async () => {
        // Just to make sure that no request goes out if the test fails
        controller.runner = () => { throw new Error(); }
        return assertReject(controller.initiateMultipartUpload({
          bucket: 'bucket',
          key: 'key',
          sha256: '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeef',
          size: 1,
          storageClass: 'INVALID',
        }));
      });

      for (let storageClass of [undefined, 'STANDARD', 'STANDARD_IA', 'REDUCED_REDUNDANCY']) {
        it('should use the correct storage class for ' + storageClass, async () => {
          let runner = sandbox.mock();
          runner.once();
          controller.runner = runner;

          runner.returns({
            body: [
              '<?xml version="1.0" encoding="UTF-8"?>',
              '<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
              '  <Bucket>bucket</Bucket>',
              '  <Key>key</Key>',
              '  <UploadId>VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>',
              '</InitiateMultipartUploadResult>',
            ].join('\n'),
            headers: {},
            statusCode: 200,
            statusMessage: 'OK',
          });

          let result = await controller.initiateMultipartUpload({
            bucket: 'bucket',
            key: 'key',
            sha256: '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeef',
            size: 1,
            storageClass: storageClass,
          });

          runner.verify();

          assume(runner.firstCall.args).is.array();
          assume(runner.firstCall.args).has.lengthOf(1);
          let arg = runner.firstCall.args[0];
          assume(arg.req).has.property('method', 'POST');
          assume(arg.req).has.property('url', 'http://localhost:8080/bucket/key?uploads=');
          assume(arg.req).has.property('headers');
          assume(arg.req.headers).has.property('x-amz-storage-class', storageClass ? storageClass : 'STANDARD');
        });
      }

      it('should set metadata headers correctly', async () => {
        let runner = sandbox.mock();
        runner.once();
        controller.runner = runner;

        runner.returns({
          body: [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
            '  <Bucket>bucket</Bucket>',
            '  <Key>key</Key>',
            '  <UploadId>VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>',
            '</InitiateMultipartUploadResult>',
          ].join('\n'),
          headers: {},
          statusCode: 200,
          statusMessage: 'OK',
        });

        let result = await controller.initiateMultipartUpload({
          bucket: 'bucket',
          key: 'key',
          sha256: '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeef',
          size: 1,
          metadata: {
            string: 'string',
            number: 123
          },
        });

        runner.verify();

        assume(runner.firstCall.args).is.array();
        assume(runner.firstCall.args).has.lengthOf(1);
        let arg = runner.firstCall.args[0];
        assume(arg.req).has.property('method', 'POST');
        assume(arg.req).has.property('url', 'http://localhost:8080/bucket/key?uploads=');
        assume(arg.req).has.property('headers');
        assume(arg.req.headers).has.property('x-amz-meta-string', 'string');
        assume(arg.req.headers).has.property('x-amz-meta-number', '123');
      });
      
      it('should set content-encoding headers correctly with gzip encoding', async () => {
        let runner = sandbox.mock();
        runner.once();
        controller.runner = runner;

        runner.returns({
          body: [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
            '  <Bucket>bucket</Bucket>',
            '  <Key>key</Key>',
            '  <UploadId>VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>',
            '</InitiateMultipartUploadResult>',
          ].join('\n'),
          headers: {},
          statusCode: 200,
          statusMessage: 'OK',
        });

        let result = await controller.initiateMultipartUpload({
          bucket: 'bucket',
          key: 'key',
          sha256: '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeef',
          contentEncoding: 'gzip',
          // NOTE This is a slightly different sha256
          transferSha256: '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeee',
          size: 1,
          transferSize: 1,
        });

        runner.verify();

        assume(runner.firstCall.args).is.array();
        assume(runner.firstCall.args).has.lengthOf(1);
        let arg = runner.firstCall.args[0];
        assume(arg.req).has.property('method', 'POST');
        assume(arg.req).has.property('url', 'http://localhost:8080/bucket/key?uploads=');
        assume(arg.req).has.property('headers');
        assume(arg.req.headers).has.property('x-amz-meta-content-sha256',
          '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeef');
        assume(arg.req.headers).has.property('x-amz-meta-transfer-sha256',
          '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeee');
        assume(arg.req.headers).has.property('content-encoding', 'gzip');
      });
      
      it('should set content-encoding headers correctly with identity encoding', async () => {
        let runner = sandbox.mock();
        runner.once();
        controller.runner = runner;

        runner.returns({
          body: [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
            '  <Bucket>bucket</Bucket>',
            '  <Key>key</Key>',
            '  <UploadId>VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>',
            '</InitiateMultipartUploadResult>',
          ].join('\n'),
          headers: {},
          statusCode: 200,
          statusMessage: 'OK',
        });

        let result = await controller.initiateMultipartUpload({
          bucket: 'bucket',
          key: 'key',
          sha256: '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeef',
          contentEncoding: 'identity',
          transferSha256: '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeef',
          size: 1,
          transferSize: 1,
        });

        runner.verify();

        assume(runner.firstCall.args).is.array();
        assume(runner.firstCall.args).has.lengthOf(1);
        let arg = runner.firstCall.args[0];
        assume(arg.req).has.property('method', 'POST');
        assume(arg.req).has.property('url', 'http://localhost:8080/bucket/key?uploads=');
        assume(arg.req).has.property('headers');
        assume(arg.req.headers).has.property('x-amz-meta-content-sha256',
          '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeef');
        assume(arg.req.headers).has.property('x-amz-meta-transfer-sha256',
          '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeef');
        assume(arg.req.headers).has.property('content-encoding', 'identity');
      });
      
      it('should set content-encoding headers correctly with no content encoding specified', async () => {
        let runner = sandbox.mock();
        runner.once();
        controller.runner = runner;

        runner.returns({
          body: [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
            '  <Bucket>bucket</Bucket>',
            '  <Key>key</Key>',
            '  <UploadId>VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>',
            '</InitiateMultipartUploadResult>',
          ].join('\n'),
          headers: {},
          statusCode: 200,
          statusMessage: 'OK',
        });

        let result = await controller.initiateMultipartUpload({
          bucket: 'bucket',
          key: 'key',
          sha256: '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeef',
          size: 1,
        });

        runner.verify();

        assume(runner.firstCall.args).is.array();
        assume(runner.firstCall.args).has.lengthOf(1);
        let arg = runner.firstCall.args[0];
        assume(arg.req).has.property('method', 'POST');
        assume(arg.req).has.property('url', 'http://localhost:8080/bucket/key?uploads=');
        assume(arg.req).has.property('headers');
        assume(arg.req.headers).has.property('x-amz-meta-content-sha256',
          '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeef');
        assume(arg.req.headers).has.property('x-amz-meta-transfer-sha256',
          '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeef');
        assume(arg.req.headers).has.property('content-encoding', 'identity');
      });

      it('should set the other content headers correctly', async () => {
        let runner = sandbox.mock();
        runner.once();
        controller.runner = runner;

        runner.returns({
          body: [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
            '  <Bucket>bucket</Bucket>',
            '  <Key>key</Key>',
            '  <UploadId>VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>',
            '</InitiateMultipartUploadResult>',
          ].join('\n'),
          headers: {},
          statusCode: 200,
          statusMessage: 'OK',
        });

        let result = await controller.initiateMultipartUpload({
          bucket: 'bucket',
          key: 'key',
          sha256: '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeef',
          size: 1,
          contentType: 'content-type',
          contentDisposition: 'content-disposition',
        });

        runner.verify();

        assume(runner.firstCall.args).is.array();
        assume(runner.firstCall.args).has.lengthOf(1);
        let arg = runner.firstCall.args[0];
        assume(arg.req).has.property('method', 'POST');
        assume(arg.req).has.property('url', 'http://localhost:8080/bucket/key?uploads=');
        assume(arg.req).has.property('headers');
        assume(arg.req.headers).has.property('content-type', 'content-type');
        assume(arg.req.headers).has.property('content-disposition', 'content-disposition');
      });

    });
  });

  describe('Metadata', () => {
    it('should work for a single object', () => {
      let actual = controller.__generateMetadataHeaders({
        a: 'string',
        b: 123,
      });

      assume(actual).to.deeply.equal({'x-amz-meta-a': 'string', 'x-amz-meta-b': '123'});
    });
    
    it('should work for multiple object', () => {
      let actual = controller.__generateMetadataHeaders({a:1}, {b:2}, {c:3});
      let expected = {
        'x-amz-meta-a': '1',
        'x-amz-meta-b': '2',
        'x-amz-meta-c': '3',
      };

      assume(actual).to.deeply.equal(expected);
    });

    it('should throw when trying to define key twice', () => {
      assume(() => {
        controller.__generateMetadataHeaders({a:1}, {a:2});
      }).throws('Attempting to define a in metadata twice');
    });

    it('should throw for a value that is too big', () => {
      let bytes = 2048 - 'x-amz-meta-a'.length;
      let max = Buffer.alloc(bytes, 'a').toString('utf8');
      controller.__generateMetadataHeaders({a: max});

      max = max + 'a';

      assume(() => {
        controller.__generateMetadataHeaders({a: max});
      }).throws(/Metadata exceeds 2048 byte/);

    });

    it('should throw a boolean value', () => {
      assume(() => {
        controller.__generateMetadataHeaders({a: true});
      }).throws(/Metadata values must be string/);
    });
    it('should throw an undefined value', () => {
      assume(() => {
        controller.__generateMetadataHeaders({a: undefined});
      }).throws(/Metadata values must be string/);
    });
    it('should throw an object value', () => {
      assume(() => {
        controller.__generateMetadataHeaders({a: {b:1}});
      }).throws(/Metadata values must be string/);
    });
    it('should throw a function value', () => {
      assume(() => {
        controller.__generateMetadataHeaders({a: () => {}});
      }).throws(/Metadata values must be string/);
    });


  });

  describe('Generate Multipart Request', () => {
    it ('should return the right values', async () => {
      let parts = [
        {sha256: crypto.createHash('sha256').update('part1').digest('hex'), size: 5*1024*1024},
        {sha256: crypto.createHash('sha256').update('part2').digest('hex'), size: 5*1024*1024},
        {sha256: crypto.createHash('sha256').update('part3').digest('hex'), size: 128},
      ];
      let result = await controller.generateMultipartRequest({
        bucket: 'example-bucket',
        key: 'example-key',
        uploadId: 'example-uploadid',
        parts: parts
      });

      let n = 1;
      for (let request of result) {
        assume(request).has.property('url', `http://localhost:8080/example-bucket/example-key?partNumber=${n}&uploadId=example-uploadid`);
        assume(request).has.property('method', 'PUT');
        assume(request).has.property('headers');
        assume(request.headers).has.property('x-amz-content-sha256', parts[n-1].sha256);
        assume(request.headers).has.property('content-length', Number(parts[n-1].size).toString(10));
        assume(request.headers).has.property('Host', 'localhost:' + port);
        assume(request.headers).has.property('Authorization');
        assume(request.headers).has.property('X-Amz-Date');
        n++;
      }

    });

    it('should have correct number of parts', async () => {
      let hash = crypto.createHash('sha256').update('hi').digest('hex');
      let parts = [];

      for (let x = 0 ; x < 10000 ; x++) {
        parts.push({
          sha256: hash,
          size: 5*1024*1024
        });
      }

      await controller.generateMultipartRequest({
        bucket: 'bucket',
        key: 'key',
        uploadId: 'uploadId',
        parts: parts,
      });

      parts.push({
        sha256: hash,
        size: 5*1024*1024
      });

      return assertReject(controller.generateMultipartRequest({
        bucket: 'bucket',
        key: 'key',
        uploadId: 'uploadId',
        parts: parts,
      }));
    });

    it('should not allow parts with size < 5MB', async () => {
      await controller.generateMultipartRequest({
        bucket: 'bucket',
        key: 'key',
        uploadId: 'uploadId',
        parts: [
          {sha256: crypto.createHash('sha256').update('hi').digest('hex'), size: 5*1024*1024},
          {sha256: crypto.createHash('sha256').update('hi').digest('hex'), size: 5*1024*1024-1},
        ],
      });
      return assertReject(controller.generateMultipartRequest({
        bucket: 'bucket',
        key: 'key',
        uploadId: 'uploadId',
        parts: [
          {sha256: crypto.createHash('sha256').update('hi').digest('hex'), size: 5*1024*1024-1},
          {sha256: crypto.createHash('sha256').update('hi').digest('hex'), size: 5*1024*1024},
        ],
      }));
    });

    it('should not allow parts with size > 5GB', async () => {
      controller.generateMultipartRequest({
        bucket: 'bucket',
        key: 'key',
        uploadId: 'uploadId',
        parts: [
          {sha256: crypto.createHash('sha256').update('hi').digest('hex'), size: 5*1024*1024*1024},
        ],
      });

      return assertReject(controller.generateMultipartRequest({
        bucket: 'bucket',
        key: 'key',
        uploadId: 'uploadId',
        parts: [
          {sha256: crypto.createHash('sha256').update('hi').digest('hex'), size: 5*1024*1024*1024+1},
        ],
      }));
    });

    it('should not allow parts with an empty string sha256', () => {
      return assertReject(controller.generateMultipartRequest({
        bucket: 'bucket',
        key: 'key',
        uploadId: 'uploadId', 
        parts: [
          {sha256: crypto.createHash('sha256').update('').digest('hex'), size: 1},
        ],
      }));
    });

    it('should not allow parts with invalid sha256', () => {
      return assertReject(controller.generateMultipartRequest({
        bucket: 'bucket',
        key: 'key',
        uploadId: 'uploadId',
        parts: [
          {sha256: 'askdlfjhasdfj', size: 1},
        ],
      }));
    });
  });

  describe('S3 Object Tag Validation', () => {
    // We want to ensure that our valid key is 128 chars that are outside of
    // the ascii range.  This ensures that count is by unicode char and not by
    // byte
    let validkey = [
      '☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭',
      '☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭',
      '☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭',
      '☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭☭',
    ].join('');
    if (validkey.length !== 128) {
      throw new Error();
    }

    // We want to add the minimal amount to the string to cause a failure. In
    // this case, it's a single char in the ascii range
    let invalidkey = validkey + 'a';

    // Valid values are just 2x a key
    let validvalue = validkey + validkey;
    let invalidvalue = validvalue + 'a';
   
    it('should not throw for valid keys and values', async () => {
      let tags = {};
      tags[validkey] = validvalue;
      controller.__validateTags(tags);
    });

    it('should throw for too many keys', () => {
      let tags = {
        a: '1',
        b: '2',
        c: '3',
        d: '4',
        e: '5',
        f: '6',
        g: '7',
        h: '8',
        i: '9',
        j: '10',
      }
      controller.__validateTags(tags);
      tags.k = '11';
      assume(() => {
        controller.__validateTags(tags);
      }).throws(/S3 allows no more than 10 tags/);
    });
 
    it('should throw for invalid keys', () => {
      assume(() => {
        let tags = {};
        tags[invalidkey] = validvalue;
        controller.__validateTags(tags);
      }).throws(/^S3 object tag keys/);
    });

    it('should throw for invalid values', () => {
      assume(() => {
        let tags = {};
        tags[validkey] = invalidvalue;
        controller.__validateTags(tags);
      }).throws(/^S3 object tag values/);
    });
  });

  describe('Generate Get Request', () => {
    it ('should return the right values for unsigned', async () => {
      let result = await controller.generateGetUrl({
        bucket: 'example-bucket',
        key: 'example-key',
      });
      assume(result).equals('http://localhost:8080/example-bucket/example-key');
    });

    it ('should return the right values for signed', async () => {
      controller.vhostAddressing = false;
      let result = await controller.generateGetUrl({
        bucket: 'example-bucket',
        key: 'example-key',
        signed: true,
      });

      result = urllib.parse(result);
      assume(result).has.property('protocol', 'http:');
      assume(result).has.property('host', 'localhost:8080');
      assume(result).has.property('pathname', '/example-bucket/example-key');
      result = qs.parse(result.query);
      for (let k of ['Expires', 'Date', 'Algorithm', 'Credential', 'SignedHeaders', 'Signature']) {
        assume(result).has.property('X-Amz-' + k);
      }
    });

  });



  describe('Generate Single Part Request', () => {
    let sha256 = crypto.createHash('sha256').update('single part').digest('hex');

    it ('should return the right values', async () => {
      let size = 1024;

      let result = await controller.generateSinglepartRequest({
        bucket: 'example-bucket',
        key: 'example-key',
        sha256: sha256,
        size: size,
        tags: {
          tag1: 'value1',
          tag2: 'value2',
        },
        permissions: {
          acl: 'public-read',
        }
      });
      
      assume(result).has.property('url', `http://localhost:8080/example-bucket/example-key`);
      assume(result).has.property('method', 'PUT');
      assume(result).has.property('headers');
      assume(result.headers).has.property('x-amz-content-sha256', sha256);
      assume(result.headers).has.property('x-amz-meta-content-sha256', sha256);
      assume(result.headers).has.property('x-amz-meta-content-length', size + '');
      assume(result.headers).has.property('content-length', size + '');
      assume(result.headers).has.property('Host', 'localhost:' + port);
      assume(result.headers).has.property('Authorization');
      assume(result.headers).has.property('X-Amz-Date');
      assume(result.headers).has.property('x-amz-acl', 'public-read');
      assume(result.headers).has.property('x-amz-tagging', 'tag1=value1&tag2=value2');
    });

    it('should support gzip content-encoding', async () => {
      let transferSha256 = crypto.createHash('sha256').update('gzip single part').digest('hex');
      let result = await controller.generateSinglepartRequest({
        bucket: 'example-bucket',
        key: 'example-key',
        sha256: sha256,
        transferSha256: transferSha256,
        size: 1024,
        transferSize: 1022,
        contentEncoding: 'gzip',
      });
      assume(result.headers).has.property('x-amz-meta-content-sha256', sha256);
      assume(result.headers).has.property('x-amz-meta-transfer-sha256', transferSha256);
      assume(result.headers).has.property('x-amz-meta-content-length', '1024');
      assume(result.headers).has.property('x-amz-meta-transfer-length', '1022');
    });

    it('should support identity content-encoding', async () => {
      let result = await controller.generateSinglepartRequest({
        bucket: 'example-bucket',
        key: 'example-key',
        sha256: sha256,
        size: 1024,
        contentEncoding: 'identity',
      });
      assume(result.headers).has.property('x-amz-meta-content-sha256', sha256);
      assume(result.headers).has.property('x-amz-meta-transfer-sha256', sha256);
      assume(result.headers).has.property('x-amz-meta-content-length', '1024');
      assume(result.headers).has.property('x-amz-meta-transfer-length', '1024');
      assume(result.headers).has.property('content-encoding', 'identity');
    });


    it('should support no specified content-encoding', async () => {
      let result = await controller.generateSinglepartRequest({
        bucket: 'example-bucket',
        key: 'example-key',
        sha256: sha256,
        size: 1024,
      });
      assume(result.headers).has.property('x-amz-meta-content-sha256', sha256);
      assume(result.headers).has.property('x-amz-meta-transfer-sha256', sha256);
      assume(result.headers).has.property('content-encoding', 'identity');
    });

    it('should throw for invalid content-encoding values with identity encoding', async () => {
      // Just to make sure that no request goes out if the test fails
      let transferSha256 = crypto.createHash('sha256').update('aaa').digest('hex');
      controller.runner = () => { throw new Error(); }
      return assertReject(controller.generateSinglepartRequest({
        bucket: 'bucket',
        key: 'key',
        sha256: sha256,
        transferSha256: transferSha256,
        size: 1,
        storageClass: 'INVALID',
      }));
    });

    it('should throw for missing transferSha256 with non-identity encoding', async () => {
      // Just to make sure that no request goes out if the test fails
      controller.runner = () => { throw new Error(); }
      return assertReject(controller.generateSinglepartRequest({
        bucket: 'bucket',
        key: 'key',
        sha256: sha256,
        contentEncoding: 'gzip',
        size: 1,
        storageClass: 'INVALID',
      }));
    });
      
    it('should throw for invalid storage classes', async () => {
      // Just to make sure that no request goes out if the test fails
      controller.runner = () => { throw new Error(); }
      return assertReject(controller.generateSinglepartRequest({
        bucket: 'bucket',
        key: 'key',
        sha256: sha256,
        size: 1,
        storageClass: 'INVALID',
      }));
    });

    for (let storageClass of [undefined, 'STANDARD', 'STANDARD_IA', 'REDUCED_REDUNDANCY']) {
      it('should use the correct storage class for ' + storageClass, async () => {
        let result = await controller.generateSinglepartRequest({
          bucket: 'bucket',
          key: 'key',
          sha256: sha256,
          size: 1,
          storageClass: storageClass,
        });

        assume(result).has.property('method', 'PUT');
        assume(result).has.property('url', 'http://localhost:8080/bucket/key');
        assume(result).has.property('headers');
        assume(result.headers).has.property('x-amz-storage-class', storageClass ? storageClass : 'STANDARD');
      });
    }

    it('should set other http headers correctly', async () => {
      let result = await controller.generateSinglepartRequest({
        bucket: 'bucket',
        key: 'key',
        sha256: sha256,
        size: 1,
        contentType: 'content-type',
        contentDisposition: 'content-disposition',
      });
      assume(result.headers).has.property('content-type', 'content-type');
      assume(result.headers).has.property('content-disposition', 'content-disposition');
    });
      
    it('should set metadata headers correctly', async () => {
      let result = await controller.generateSinglepartRequest({
        bucket: 'bucket',
        key: 'key',
        sha256: sha256,
        size: 1,
        metadata: {
          string: 'string',
          number: 123,
        }
      });
      assume(result.headers).has.property('x-amz-meta-string', 'string');
      assume(result.headers).has.property('x-amz-meta-number', '123');
    });

    it('should not allow size <= 0', () => {
      return assertReject(controller.generateSinglepartRequest({
        bucket: 'bucket',
        key: 'key',
        sha256: '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeef',
        size: 0,
      }));
    });

    it('should not allow an empty string sha256', () => {
      return assertReject(controller.generateSinglepartRequest({
        bucket: 'bucket',
        key: 'key',
        sha256: crypto.createHash('sha256').update('').digest('hex'),
        size: 1,
      }));
    });

    it('should not allow invalid sha256', () => {
      return assertReject(controller.generateSinglepartRequest({
        bucket: 'bucket',
        key: 'key',
        sha256: 'asdflasdf',
        size: 1,
      }));
    });
  });

  describe('S3 Permissions', () => {
    it('should handle a valid Canned ACL', () => {
      let actual = controller.__determinePermissionsHeaders({acl: 'private'});
      let expected = [['x-amz-acl', 'private']];
      assume(actual).deeply.equals(expected);
    });

    it('should handle an invalid Canned ACL', () => {
      assume(() => {
        controller.__determinePermissionsHeaders({acl: 'bogus'});
      }).throws(/^child "acl" fails because \["acl" must be one of \[/);
    });

    it('should treat canned ACLs and specific permissions as mutually exclusive', () => {
      assume(() => {
        controller.__determinePermissionsHeaders({acl: 'private', read: 'ooogieboogie'});
      }).throws(/^"acl" conflict with forbidden peer "read"/);
    });

    it('should handle specific permissions', () => {
      let permissions = {
        read: 'x-amz-grant-read',
        readAcp: 'x-amz-grant-read-acp',
        write: 'x-amz-grant-write',
        writeAcp: 'x-amz-grant-write-acp',
        fullControl: 'x-amz-grant-full-control',
      };

      let actual = controller.__determinePermissionsHeaders(permissions);
      
      for (let tuple of actual) {
        assume(tuple[0]).equals(tuple[1]);
      }
    });
  });
});

