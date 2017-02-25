let assume = require('assume');
let { Controller, run, parseS3Response } = require('../lib/controller');

describe('XML Parsing', () => {

  it('should return undefined when given empty body', () => {
    let actual = parseS3Response('');
    assume(actual).to.be.an('undefined');
  });

  it('should parse a generic response body into an xml document', () => {
    let body = [
      '<?xml version="1.0" encoding="UTF-8"?>',
      '<InitiateMultipartUploadResult',
      'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
      '   <Bucket>example-bucket</Bucket>',
      '   <Key>example-object</Key>',
      '   <UploadId>snip</UploadId>',
      '</InitiateMultipartUploadResult> ',
    ].join('\n');

    let doc = parseS3Response(body);
    assume(doc.root).to.be.a('function');
  });

  it('should understand general s3 responses', () => {
    let s3 = new Controller({region: 'us-east-1'});
    let body = [
      '<?xml version="1.0" encoding="UTF-8"?>',
      '<container',
      'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
      '   <Bucket>b</Bucket>',
      '   <Key>k</Key>',
      '   <property>lookingfor</property>',
      '</container> ',
    ].join('\n');

    let doc = parseS3Response(body);

    assume(s3.__getResponseProperty(doc, 'container', 'property', 'b', 'k')).equals('lookingfor');

    assume(() => {
      s3.__getResponseProperty(doc, 'container', 'property', 'c', 'k');
    }).to.throw(/^Document contains incorrect Bucket/)

    assume(() => {
      s3.__getResponseProperty(doc, 'container', 'property', 'b', 'd');
    }).to.throw(/^Document contains incorrect Key/)

    assume(() => {
      s3.__getResponseProperty(doc, 'alskdfj', 'property', 'b', 'd');
    }).to.throw(/^Document does not have/)

 
  });

  it('should only parse initiate multipart upload bodies', () => {
    let s3 = new Controller({region: 'us-east-1'});

    let body = [
      '<?xml version="1.0" encoding="UTF-8"?>',
      '<InitiateMultipartUploadResult',
      'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
      '   <Bucket>example-bucket</Bucket>',
      '   <Key>example-object</Key>',
      '</InitiateMultipartUploadResult> ',
    ].join('\n');

    let doc = parseS3Response(body);

    assume(() => {
      s3.__getUploadId(doc, 'incorrect-bucket', 'example-object');
    }).to.throw(/^Document contains incorrect Bucket/);

    assume(() => {
      s3.__getUploadId(doc, 'example-bucket', 'incorrect-object');
    }).to.throw(/^Document contains incorrect Key/);

    assume(() => {
      s3.__getUploadId(doc, 'example-bucket', 'example-object');
    }).to.throw(/^Document does not contain UploadId/);

  });

  it('should parse an initiate multipart upload body', () => {
    let s3 = new Controller({region: 'us-east-1'});

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

    let doc = parseS3Response(body);

    let actual = s3.__getUploadId(doc, 'example-bucket', 'example-object');
    assume(actual).equals(expected);
  });

  it('should parse a general S3 error correctly', () => {
    let body = [
      '<?xml version="1.0" encoding="UTF-8"?>',
      '<Error>',
      '  <Code>mycode</Code>',
      '  <Message>mymessage</Message>',
      '  <Resource>myresource</Resource> ',
      '  <RequestId>myrequestid</RequestId>',
      '</Error>',
    ].join('\n');

    let err = parseS3Response(body, true);
    assume(err).to.be.instanceof(Error);
    assume(err).has.property('code', 'mycode');
    assume(err).has.property('message', 'mymessage');
    assume(err).has.property('resource', 'myresource');
    assume(err).has.property('requestid', 'myrequestid');
    
    assume(() => {
      parseS3Response(body);
    }).to.throw(/^mymessage$/);


  });

  // I can't find a good example body to test against, but since
  // this is just the standard error parsing but checking for
  // a couple extra properties it's not critical
  it.skip('should parse an invalid signature S3 error correctly', () => {
  });
});

describe('XML Generation', () => {
  it('should generate valid content', () => {
    let s3 = new Controller({region: 'us-east-1'});

    let expected = [
      '<?xml version="1.0" encoding="UTF-8"?>',
      '<CompleteMultipartUpload>',
      '  <Part>',
      '    <PartNumber>1</PartNumber>',
      '    <ETag>a</ETag>',
      '  </Part>',
      '  <Part>',
      '    <PartNumber>2</PartNumber>',
      '    <ETag>b</ETag>',
      '  </Part>',
      '  <Part>',
      '    <PartNumber>3</PartNumber>',
      '    <ETag>c</ETag>',
      '  </Part>',
      '</CompleteMultipartUpload>',
    ].join('\n').trim();

    let actual = s3.__generateCompleteUploadBody(['a', 'b', 'c']).trim();

    assume(actual).equals(expected);
  });

});
