const crypto = require('crypto');
const assume = require('assume');
const sinon = require('sinon');
const { Controller } = require('../');
const assertReject = require('./utils').assertReject;
const InterchangeFormat = require('../lib/interchange-format');

// Factory method for making S3 objects
function runner () {
  let mock = sinon.mock();
  mock.onSecondCall().throws(new Error('Only call runner once!'));
  let inst = new Controller({region: 'us-east-1', runner: mock});
  return {mock, inst};
}

async function checkRunner(mock, opts) {
  let url = opts.url;
  let method = opts.method;
  // A list of keys that *must* be there
  let headerKeys = opts.headerKeys || [];
  // For all key value pairings, ensure that the key
  // is either the string value or .test()'s true for regexp
  let headerValues = opts.headerValues || {};

  assume(mock.firstCall.args).to.have.lengthOf(1);
  let options = mock.firstCall.args[0];
  let body = options.body;
  let req = options.req;
  let streamingOutput = options.streaming;

  if (typeof opts.body !== 'undefined') {
    assume(opts.body).equals(body);
  }

  if (body) {
    let bodysha256 = crypto.createHash('sha256').update(body).digest('hex');
    if (req.headers['x-amz-content-sha256']) {
      assume(req.headers['x-amz-content-sha256']).equals(bodysha256);
    } else if (req.headers['X-Amz-Content-Sha256']) {
      assume(req.headers['X-Amz-Content-Sha256']).equals(bodysha256);
    } else {
      throw new Error('You have a body without a content sha256');
    }
  }

  await InterchangeFormat.validate(req);

  assume(req).to.be.an('object');

  for (let key of headerKeys) {
    assume(req.headers).has.property(key);
  }
  for (let key in headerValues) {
    assume(req.headers).has.property(key, headerValues[key]);
  }

  assume(mock.calledOnce).to.be.true;
}

describe('S3 Client', () => {
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

  describe('Initiate Multipart Upload', () => {
    it('should call run with the correct arguments', async () => {
      let body = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<InitiateMultipartUploadResult',
        'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
        '   <Bucket>example-bucket</Bucket>',
        '   <Key>example-object</Key>',
        '   <UploadId>EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-</UploadId>',
        '</InitiateMultipartUploadResult> ',
      ].join('\n');

      let expected = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'

      let {mock, inst} = runner();

      mock.returns({
        body: body,
        headers: {},
        statusCode: 200,
      });

      // rando sha256
      let result = await inst.initiateMultipartUpload({
        bucket: 'example-bucket',
        key: 'example-object',
        uploadId: 'testsha256',
        sha256: '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeef',
        size: 1234,
      });

      // Note that since we're not sending a content body, we're always going to check that
      // the x-amz-content-sha256 value is the sha256 of the empty string
      await checkRunner(mock, {
        url: 'https://example-bucket.s3.amazonaws.com/example-object?uploads=',
        method: 'POST',
        headerKeys: ['X-Amz-Date', 'Authorization'],
        headerValues: {
          'x-amz-meta-taskcluster-content-sha256': '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeef',
          'x-amz-meta-taskcluster-content-length': 1234,
          Host: 'example-bucket.s3.amazonaws.com',
          'X-Amz-Content-Sha256': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
        }
      });
    });

    it('should not allow parts with size <= 0', () => {
      let {inst} = runner();

      return assertReject(inst.initiateMultipartUpload({
        bucket: 'bucket',
        key: 'key',
        uploadId: 'uploadId',
        sha256: '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeef',
        size: 0,
      }));
    });

    it('should not allow parts with an empty string sha256', () => {
      let {inst} = runner();

      return assertReject(inst.initiateMultipartUpload({
        bucket: 'bucket',
        key: 'key',
        uploadId: 'uploadId',
        sha256: crypto.createHash('sha256').update('').digest('hex'),
        size: 1,
      }));
    });

    it('should not allow parts with invalid sha256', () => {
      let {inst} = runner();

      return assertReject(inst.initiateMultipartUpload({
        bucket: 'bucket',
        key: 'key',
        uploadId: 'uploadId',
        sha256: 'asdflasdf',
        size: 1,
      }));
    });
  });

  describe('Complete Multipart Upload', () => {
    it('should call run with the correct arguments', async () => {
      let {mock, inst} = runner();

      let etags = ['etag1', 'etag2'];
      
      let body = inst.__generateCompleteUploadBody(etags);
      let bodyHash = crypto.createHash('sha256').update(body).digest('hex');

      let responseBody = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
        '  <Location>http://Example-Bucket.s3.amazonaws.com/Example-Object</Location>',
        '  <Bucket>example-bucket</Bucket>',
        '  <Key>example-object</Key>',
        '  <ETag>"3858f62230ac3c915f300c664312c11f-9"</ETag>',
        '</CompleteMultipartUploadResult>',
      ].join('\n');

      mock.returns({
        body: responseBody,
        headers: {},
        statusCode: 200,
      });

      let result = await inst.completeMultipartUpload({
        bucket: 'example-bucket',
        key: 'example-object',
        uploadId: 'myuploadid',
        etags: etags,
      });

      // Note that since we're not sending a content body, we're always going to check that
      // the x-amz-content-sha256 value is the sha256 of the empty string
      await checkRunner(mock, {
        url: 'https://example-bucket.s3.amazonaws.com/example-object?uploadId=myuploadid',
        method: 'POST',
        headerKeys: ['X-Amz-Date', 'Authorization'],
        body: body,
        headerValues: {
          Host: 'example-bucket.s3.amazonaws.com',
          'X-Amz-Content-Sha256': bodyHash,
        }
      });
    });
  });
  
  describe('Abort Multipart Upload', () => {
    it('should call run with the correct arguments', async () => {
      let {mock, inst} = runner();

      mock.returns({
        body: '',
        headers: {},
        statusCode: 204,
      });

      let result = await inst.abortMultipartUpload({
        bucket: 'example-bucket',
        key: 'example-object',
        uploadId: 'myuploadid',
      });

      // Note that since we're not sending a content body, we're always going to check that
      // the x-amz-content-sha256 value is the sha256 of the empty string
      await checkRunner(mock, {
        url: 'https://example-bucket.s3.amazonaws.com/example-object?uploadId=myuploadid',
        method: 'DELETE',
        headerKeys: ['X-Amz-Date', 'Authorization'],
        headerValues: {
          Host: 'example-bucket.s3.amazonaws.com',
        }
      });
    });
  });

  describe('Generate Multipart Request', () => {
    it ('should return the right values', async () => {
      let {inst} = runner();
      let parts = [
        {sha256: crypto.createHash('sha256').update('part1').digest('hex'), size: 5*1024*1024},
        {sha256: crypto.createHash('sha256').update('part2').digest('hex'), size: 5*1024*1024},
        {sha256: crypto.createHash('sha256').update('part3').digest('hex'), size: 128},
      ];
      let result = await inst.generateMultipartRequest({
        bucket: 'example-bucket',
        key: 'example-key',
        uploadId: 'example-uploadid',
        parts: parts
      });

      let n = 1;
      for (let request of result) {
        assume(request).has.property('url', `https://example-bucket.s3.amazonaws.com/example-key?partNumber=${n}&uploadId=example-uploadid`);
        assume(request).has.property('method', 'PUT');
        assume(request).has.property('headers');
        assume(request.headers).has.property('x-amz-content-sha256', parts[n-1].sha256);
        assume(request.headers).has.property('content-length', parts[n-1].size);
        assume(request.headers).has.property('Host', 'example-bucket.s3.amazonaws.com');
        assume(request.headers).has.property('Authorization');
        assume(request.headers).has.property('X-Amz-Date');
        n++;
      }

    });

    it('should have correct number of parts', async () => {
      let {inst} = runner();

      let hash = crypto.createHash('sha256').update('hi').digest('hex');
      let parts = [];

      for (let x = 0 ; x < 10000 ; x++) {
        parts.push({
          sha256: hash,
          size: 5*1024*1024
        });
      }

      await inst.generateMultipartRequest({
        bucket: 'bucket',
        key: 'key',
        uploadId: 'uploadId',
        parts: parts,
      });

      parts.push({
        sha256: hash,
        size: 5*1024*1024
      });

      return assertReject(inst.generateMultipartRequest({
        bucket: 'bucket',
        key: 'key',
        uploadId: 'uploadId',
        parts: parts,
      }));
    });

    it('should not allow parts with size < 5MB', async () => {
      let {inst} = runner();

      await inst.generateMultipartRequest({
        bucket: 'bucket',
        key: 'key',
        uploadId: 'uploadId',
        parts: [
          {sha256: crypto.createHash('sha256').update('hi').digest('hex'), size: 5*1024*1024},
          {sha256: crypto.createHash('sha256').update('hi').digest('hex'), size: 5*1024*1024-1},
        ],
      });
      return assertReject(inst.generateMultipartRequest({
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
      let {inst} = runner();

      inst.generateMultipartRequest({
        bucket: 'bucket',
        key: 'key',
        uploadId: 'uploadId',
        parts: [
          {sha256: crypto.createHash('sha256').update('hi').digest('hex'), size: 5*1024*1024*1024},
        ],
      });

      return assertReject(inst.generateMultipartRequest({
        bucket: 'bucket',
        key: 'key',
        uploadId: 'uploadId',
        parts: [
          {sha256: crypto.createHash('sha256').update('hi').digest('hex'), size: 5*1024*1024*1024+1},
        ],
      }));
    });

    it('should not allow parts with an empty string sha256', () => {
      let {inst} = runner();

      return assertReject(inst.generateMultipartRequest({
        bucket: 'bucket',
        key: 'key',
        uploadId: 'uploadId', 
        parts: [
          {sha256: crypto.createHash('sha256').update('').digest('hex'), size: 1},
        ],
      }));
    });

    it('should not allow parts with invalid sha256', () => {
      let {inst} = runner();

      return assertReject(inst.generateMultipartRequest({
        bucket: 'bucket',
        key: 'key',
        uploadId: 'uploadId',
        parts: [
          {sha256: 'askdlfjhasdfj', size: 1},
        ],
      }));
    });
  });

  describe('Tag an Object', () => {
    it ('should call run with the correct arguments', async () => {
      let {mock, inst} = runner();

      let body = inst.__generateTagSetBody({tag1: 'value1'});

      mock.returns({
        body: body,
        headers: {},
        statusCode: 200,
      });

      await inst.__tagObject({
        bucket: 'example-bucket',
        key: 'example-object',
        tags: {tag1: 'value1'},
      });

      await checkRunner(mock, {
        url: 'https://example-bucket.s3.amazonaws.com/example-object?tagging=',
        method: 'POST',
        headerKeys: ['X-Amz-Date', 'Authorization'],
        headerValues: {
          Host: 'example-bucket.s3.amazonaws.com',
        }
      });     

    });
  });
    
  describe('Generate Single Part Request', () => {
    it ('should return the right values', async () => {
      let {inst} = runner();
      let sha256 = crypto.createHash('sha256').update('part1').digest('hex');
      let size = 1024;

      let result = await inst.generateSinglepartRequest({
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
      
      assume(result).has.property('url', `https://example-bucket.s3.amazonaws.com/example-key`);
      assume(result).has.property('method', 'PUT');
      assume(result).has.property('headers');
      assume(result.headers).has.property('x-amz-content-sha256', sha256);
      assume(result.headers).has.property('x-amz-meta-taskcluster-content-sha256', sha256);
      assume(result.headers).has.property('x-amz-meta-taskcluster-content-length', size);
      assume(result.headers).has.property('content-length', size);
      assume(result.headers).has.property('Host', 'example-bucket.s3.amazonaws.com');
      assume(result.headers).has.property('Authorization');
      assume(result.headers).has.property('X-Amz-Date');
      assume(result.headers).has.property('x-amz-acl', 'public-read');
      assume(result.headers).has.property('x-amz-tagging', 'tag1=value1&tag2=value2');
    });
  });

  it('should not allow parts with size <= 0', () => {
    let {inst} = runner();

    return assertReject(inst.generateSinglepartRequest({
      bucket: 'bucket',
      key: 'key',
      sha256: '605056c0bdc0b2c9d1e32146eac54fe22a807e14b1af34f3d4343f88e592eeef',
      size: 0,
    }));
  });

  it('should not allow parts with an empty string sha256', () => {
    let {inst} = runner();

    return assertReject(inst.generateSinglepartRequest({
      bucket: 'bucket',
      key: 'key',
      sha256: crypto.createHash('sha256').update('').digest('hex'),
      size: 1,
    }));
  });

  it('should not allow parts with invalid sha256', () => {
    let {inst} = runner();

    return assertReject(inst.generateSinglepartRequest({
      bucket: 'bucket',
      key: 'key',
      sha256: 'asdflasdf',
      size: 1,
    }));
  });

  describe('S3 Permissions', () => {
    let {inst} = runner();

    it('should handle a valid Canned ACL', () => {
      let actual = inst.__determinePermissionsHeaders({acl: 'private'});
      let expected = [['x-amz-acl', 'private']];
      assume(actual).deeply.equals(expected);
    });

    it('should handle an invalid Canned ACL', () => {
      assume(() => {
        inst.__determinePermissionsHeaders({acl: 'bogus'});
      }).throws(/^You are requesting a canned ACL that is not valid/);
    });

    it('should treat canned ACLs and specific permissions as mutually exclusive', () => {
      assume(() => {
        inst.__determinePermissionsHeaders({acl: 'private', read: 'ooogieboogie'});
      }).throws(/^If you are using a canned ACL, you may not/);
    });

    it('should handle specific permissions', () => {
      let permissions = {
        read: 'x-amz-grant-read',
        readAcp: 'x-amz-grant-read-acp',
        write: 'x-amz-grant-write',
        writeAcp: 'x-amz-grant-write-acp',
        fullControl: 'x-amz-grant-full-control',
      };

      let actual = inst.__determinePermissionsHeaders(permissions);
      
      for (let tuple of actual) {
        assume(tuple[0]).equals(tuple[1]);
      }
    });


  });
});

