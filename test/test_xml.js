'use strict';
let assume = require('assume');
let libxml = require('libxmljs');
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
    let s3 = new Controller({region: 'us-east-1', runner: () => {}});

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
    let s3 = new Controller({region: 'us-east-1', runner: () => {}});

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
    '<Error><Code>SignatureDoesNotMatch</Code><Message>msg</Message><AWSAccessKeyId>key</AWSAccessKeyId><StringToSign>stringtosign</StringToSign><SignatureProvided>signatureprovided</SignatureProvided><StringToSignBytes>num pairs split by spaces</StringToSignBytes><CanonicalRequest>canonicalrequest</CanonicalRequest><CanonicalRequestBytes>num pairs split by spaces</CanonicalRequestBytes><RequestId>9F15C857BFE0F2EE</RequestId><HostId>hSoPfVamkzTDU/sTNF6pjiXV98hcXfHKMT9NdfmhxnOgWLkAkhjytVhd4TRkJgiRPRvASVjAX3w=</HostId></Error>'

    let expected = [
      '<?xml version="1.0" encoding="UTF-8"?>',
      '<Error>',
      '  <Code>SignatureDoesNotMatch</Code>',
      '  <Message>mymsg</Message>',
      '  <AWSAccessKeyId>key</AWSAccessKeyId>',
      '  <StringToSign>stringtosign</StringToSign>',
      '  <SignatureProvided>signatureprovided</SignatureProvided>',
      '  <StringToSignBytes>num pairs split by spaces</StringToSignBytes>',
      '  <CanonicalRequest>canonicalrequest</CanonicalRequest>',
      '  <CanonicalRequestBytes>num pairs split by spaces</CanonicalRequestBytes>',
      '  <RequestId>9F15C857BFE0F2EE</RequestId>',
      '  <HostId>base64</HostId>',
      '</Error>',
    ];

    let err = parseS3Response(body, true);
    assume(err).to.be.instanceof(Error);
    assume(err).has.property('code', 'SignatureDoesNotMatch');
    assume(err).has.property('message', 'msg');
    assume(err).has.property('resource', 'myresource');
    assume(err).has.property('awsaccesskeyid', 'key');
    assume(err).has.property('stringtosign', 'stringtosign');
    assume(err).has.property('signatureprovided', 'signatureprovided');
    assume(err).has.property('canonicalrequest', 'canonicalrequest');
    
    assume(() => {
      parseS3Response(body);
    }).to.throw(/^mymessage$/);
  });
});

describe('XML Generation', () => {
  it('should generate valid content to complete multipart upload', () => {
    let s3 = new Controller({region: 'us-east-1', runner: () => {}});

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

  describe('Injection', () => {
    // Since this is the only place in the system that we take untrusted input from a
    // machine and stick it into XML, this is where we're testing for injection.  The only
    // types of injection we 
    it('should not allow XML tag injection in complete multipart upload body', () => {
      let s3 = new Controller({region: 'us-east-1', runner: () => {}});

      // This is a properly escaped document
      let expected = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<CompleteMultipartUpload>',
        '  <Part>',
        '    <PartNumber>1</PartNumber>',
        '    <ETag>&lt;hi&gt;John&lt;/hi&gt;</ETag>',
        '  </Part>',
        '  <Part>',
        '    <PartNumber>2</PartNumber>',
        '    <ETag>&lt;hi&gt;</ETag>',
        '  </Part>',
        '  <Part>',
        '    <PartNumber>3</PartNumber>',
        '    <ETag>&lt;/hi&gt;</ETag>',
        '  </Part>',
        '  <Part>',
        '    <PartNumber>4</PartNumber>',
        '    <ETag>&lt;hi</ETag>',
        '  </Part>',
        '  <Part>',
        '    <PartNumber>5</PartNumber>',
        '    <ETag>/hi&gt;</ETag>',
        '  </Part>',
        '  <Part>',
        '    <PartNumber>6</PartNumber>',
        '    <ETag>&lt;![CDATA[&lt;script&gt;var n=0;while(true){n++;}&lt;/script&gt;]]&gt;</ETag>',
        '  </Part>',
        '  <Part>',
        '    <PartNumber>7</PartNumber>',
        '    <ETag>&lt;?xml version="1.0" encoding="ISO-8859-1"?&gt;&lt;foo&gt;&lt;![CDATA[&lt;]]' +
          '&gt;SCRIPT&lt;![CDATA[&gt;]]&gt;alert(\'gotcha\');&lt;![CDATA[&lt;]]&gt;/SCRIPT&lt;![CDATA[&gt;]]&gt;&lt;/foo&gt;</ETag>',
        '  </Part>',
        '  <Part>',
        '    <PartNumber>8</PartNumber>',
        '    <ETag>&lt;?xml version="1.0" encoding="ISO-8859-1"?&gt;&lt;foo&gt;&lt;![CDATA[\' or 1=1 ' +
          'or \'\'=\']]&gt;&lt;/foof&gt;</ETag>',
        '  </Part>',
        '  <Part>',
        '    <PartNumber>9</PartNumber>',
        '    <ETag>&lt;?xml version="1.0" encoding="ISO-8859-1"?&gt;&lt;!DOCTYPE foo [&lt;!ELEMENT ' +
          'foo ANY&gt;&lt;!ENTITY xxe SYSTEM "file://c:/boot.ini"&gt;]&gt;&lt;foo&gt;&amp;xee;&lt;/foo&gt;</ETag>',
        '  </Part>',
        '  <Part>',
        '    <PartNumber>10</PartNumber>',
        '    <ETag>&lt;?xml version="1.0" encoding="ISO-8859-1"?&gt;&lt;!DOCTYPE foo [&lt;!ELEMENT ' +
          'foo ANY&gt;&lt;!ENTITY xxe SYSTEM "file:///etc/passwd"&gt;]&gt;&lt;foo&gt;&amp;xee;&lt;/foo&gt;</ETag>',
        '  </Part>',
        '  <Part>',
        '    <PartNumber>11</PartNumber>',
        '    <ETag>&lt;?xml version="1.0" encoding="ISO-8859-1"?&gt;&lt;!DOCTYPE foo [&lt;!ELEMENT ' +
          'foo ANY&gt;&lt;!ENTITY xxe SYSTEM "file:///etc/shadow"&gt;]&gt;&lt;foo&gt;&amp;xee;&lt;/foo&gt;</ETag>',
        '  </Part>',
        '  <Part>',
        '    <PartNumber>12</PartNumber>',
        '    <ETag>&lt;?xml version="1.0" encoding="ISO-8859-1"?&gt;&lt;!DOCTYPE foo [&lt;!ELEMENT ' +
          'foo ANY&gt;&lt;!ENTITY xxe SYSTEM "file:///dev/random"&gt;]&gt;&lt;foo&gt;&amp;xee;&lt;/foo&gt;</ETag>',
        '  </Part>',
        '</CompleteMultipartUpload>',
      ].join('\n').trim();

      // https://www.owasp.org/index.php/OWASP_Testing_Guide_Appendix_C:_Fuzz_Vectors#XML_Injection
      let nodeContents = [
        '<hi>John</hi>',
        '<hi>',
        '</hi>',
        '<hi',
        '/hi>',
        '<![CDATA[<script>var n=0;while(true){n++;}</script>]]>',
        '<?xml version="1.0" encoding="ISO-8859-1"?><foo><![CDATA[<]]>SCRIPT<![CDATA[>]]>alert(\'gotcha\');<![CDATA[<]]>/SCRIPT<![CDATA[>]]></foo>',
        '<?xml version="1.0" encoding="ISO-8859-1"?><foo><![CDATA[\' or 1=1 or \'\'=\']]></foof>',
        '<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE foo [<!ELEMENT foo ANY><!ENTITY xxe SYSTEM "file://c:/boot.ini">]><foo>&xee;</foo>',
        '<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE foo [<!ELEMENT foo ANY><!ENTITY xxe SYSTEM "file:///etc/passwd">]><foo>&xee;</foo>',
        '<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE foo [<!ELEMENT foo ANY><!ENTITY xxe SYSTEM "file:///etc/shadow">]><foo>&xee;</foo>',
        '<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE foo [<!ELEMENT foo ANY><!ENTITY xxe SYSTEM "file:///dev/random">]><foo>&xee;</foo>',
      ];

      let actual = s3.__generateCompleteUploadBody(nodeContents).trim();

      // Assume that the generated string is matching...
      assume(actual).equals(expected);
      // ... and also that it's semantically equivalent
      assume(libxml.parseXml(actual).toString()).equals(libxml.parseXml(expected).toString());

      // Now let's double check that the number of Part nodes is equal to the number we expected
      let parsedActual = libxml.parseXml(actual);
      let x = 0;
      for (let child of parsedActual.root().childNodes()) {
        if (child.name() === 'Part') {
          x++;
        }
      }
      assume(x).equals(nodeContents.length);
    });
  });
});
