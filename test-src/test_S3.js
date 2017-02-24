let crypto = require('crypto');
let assume = require('assume');
let sinon = require('sinon');
let { S3 } = require('../');

process.on('unhandledRejection', err => {
  console.log(err);
});

// Factory method for making S3 objects
function runner () {
  let mock = sinon.mock();
  mock.onSecondCall().throws(new Error('Only call runner once!'));
  let inst = new S3('us-east-1', mock);
  return {mock, inst};
}

function checkRunner(mock, opts) {
  let url = opts.url;
  let method = opts.method;
  // A list of keys that *must* be there
  let headerKeys = opts.headerKeys || [];
  // For all key value pairings, ensure that the key
  // is either the string value or .test()'s true for regexp
  let headerValues = opts.headerValues || {};

  let obj = mock.firstCall.args[0];
  let body = mock.firstCall.args[1];

  if (typeof opts.body !== 'undefined') {
    assume(mock.firstCall.args).to.have.lengthOf(2);
    assume(opts.body).equals(body);
  } else {
    assume(mock.firstCall.args).to.have.lengthOf(1);
  }

  assume(obj).has.property('url', obj.url);
  assume(obj).has.property('method', opts.method);

  for (let key of headerKeys) {
    assume(obj.headers).has.property(key);
  }
  for (let key in headerValues) {
    assume(obj.headers).has.property(key, headerValues[key]);
  }

  assume(mock.calledOnce).to.be.true;
}

describe('S3 Client', () => {
  describe('API Hosts', () => {
    it('should use correct host for us-east-1', () => {
      let s3 = new S3('us-east-1');
      assume(s3).has.property('s3host', 's3.amazonaws.com');
    });

    it('should use correct host for us-west-1', () => {
      let s3 = new S3('us-west-1');
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
      });

      let result = await inst.initiateMultipartUpload('example-bucket', 'example-object', 'testsha256', 1234);

      // Note that since we're not sending a content body, we're always going to check that
      // the x-amz-content-sha256 value is the sha256 of the empty string
      checkRunner(mock,{
        url: 'https://example-bucket.s3.amazonaws.com/example-object?uploads=',
        method: 'POST',
        headerKeys: ['X-Amz-Date', 'Authorization'],
        headerValues: {
          'x-amz-meta-taskcluster-content-sha256': 'testsha256',
          'x-amz-meta-taskcluster-content-length': 1234,
          Host: 'example-bucket.s3.amazonaws.com',
          'X-Amz-Content-Sha256': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
        }
      });
    });
  });

  describe('Complete Multipart Upload', () => {
    it('should call run with the correct arguments', async () => {
      let {mock, inst} = runner();

      let etags = ['etag1', 'etag2'];
      
      let body = inst.__generateCompleteUploadBody(etags);
      let bodyHash = crypto.createHash('sha256').update(body).digest('hex');

      mock.returns({
        body: '',
        headers: {},
      });

      let result = await inst.completeMultipartUpload('example-bucket', 'example-object', 'testsha256', etags);

      // Note that since we're not sending a content body, we're always going to check that
      // the x-amz-content-sha256 value is the sha256 of the empty string
      checkRunner(mock, {
        url: 'https://example-bucket.s3.amazonaws.com/example-object?uploads=',
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
      });

      let result = await inst.abortMultipartUpload('example-bucket', 'example-object', 'myuploadid');

      // Note that since we're not sending a content body, we're always going to check that
      // the x-amz-content-sha256 value is the sha256 of the empty string
      checkRunner(mock, {
        url: 'https://example-bucket.s3.amazonaws.com/example-object?uploadId=myuploadid',
        method: 'DELETE',
        headerKeys: ['X-Amz-Date', 'Authorization'],
        headerValues: {
          Host: 'example-bucket.s3.amazonaws.com',
        }
      });
    });
  });

});

