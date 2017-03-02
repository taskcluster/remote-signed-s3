const crypto = require('crypto');
const http = require('http');
//const https = require('https');
const urllib = require('url');
const fs = require('fs');

const assume = require('assume');
const { Controller, parseS3Response } = require('../');
const DigestStream = require('../lib/digest-stream');
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
    controller = new Controller();
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
      let s3 = new Controller({region: 'us-east-1'});
      assume(s3).has.property('s3host', 's3.amazonaws.com');
    });

    it('should use correct host for us-west-1', () => {
      let s3 = new Controller({region: 'us-west-1'});
      assume(s3).has.property('s3host', 's3-us-west-1.amazonaws.com');
    });
  });

  describe('API Special checks', () => {
    describe('Initiate Multipart Upload', () => {
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
        assume(request).has.property('url', `http://example-bucket.localhost:8080/example-key?partNumber=${n}&uploadId=example-uploadid`);
        assume(request).has.property('method', 'PUT');
        assume(request).has.property('headers');
        assume(request.headers).has.property('x-amz-content-sha256', parts[n-1].sha256);
        assume(request.headers).has.property('content-length', parts[n-1].size);
        assume(request.headers).has.property('Host', 'example-bucket.localhost:' + port);
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

  describe('Generate Single Part Request', () => {
    it ('should return the right values', async () => {
      
      let sha256 = crypto.createHash('sha256').update('part1').digest('hex');
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
      
      assume(result).has.property('url', `http://example-bucket.localhost:8080/example-key`);
      assume(result).has.property('method', 'PUT');
      assume(result).has.property('headers');
      assume(result.headers).has.property('x-amz-content-sha256', sha256);
      assume(result.headers).has.property('x-amz-meta-content-sha256', sha256);
      assume(result.headers).has.property('x-amz-meta-content-length', size);
      assume(result.headers).has.property('content-length', size);
      assume(result.headers).has.property('Host', 'example-bucket.localhost:' + port);
      assume(result.headers).has.property('Authorization');
      assume(result.headers).has.property('X-Amz-Date');
      assume(result.headers).has.property('x-amz-acl', 'public-read');
      assume(result.headers).has.property('x-amz-tagging', 'tag1=value1&tag2=value2');
    });

    it('should not allow size <= 0', () => {
      

      return assertReject(controller.generateSinglepartRequest({
        bucket: 'bucket',
        key: 'key',
        sha256: '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeef',
        size: 0,
      }));
    });

    it('should not allow parts with an empty string sha256', () => {
      

      return assertReject(controller.generateSinglepartRequest({
        bucket: 'bucket',
        key: 'key',
        sha256: crypto.createHash('sha256').update('').digest('hex'),
        size: 1,
      }));
    });

    it('should not allow parts with invalid sha256', () => {
      

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

