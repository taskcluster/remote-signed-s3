# Remotely Signed S3 Requests
This is a library and tool designed to support doing Amazon S3 uploads and
downloads with request signing occuring on a different machine than the machine
performing the actual upload or download.

With this library, you can allow untrusted hosts to upload to an object which
you've protected with whichever authentication and authorization scheme you
choose.

There is support for both multipart and singlepart uploads and downloads.  The
high level interfaces provided will automatically select which version to use.

## Architecture
This project is divided into a server-side component called the `Controller`.
Instances of the `Controller` know how to genereate and sign all the methods
involved in uploading files to S3.  `Controller`s also know how to run methods
which must be run from the server.  The methods which must be run from the
server are the initate, complete and abort methods of the multipart uploads.

There is a `Runner` class which is used by the `Controller` to run the requests
which occur on the server.  This same `Runner` class is used to run requests on
the untrusted host.  This is done to share code as well as ensure that the code
which is published for consumers is working.

The interchange format for the `Runner` class is like so:

```json
{
  "url": "https://www.hostname.com:443/path/to/resource?query=string",
  "method": "GET",
  "headers": {
    "Content-Length": "1234",
  }
}
```

Note that the request body itself is not specified anywhere in this format.

TODO: Write some stuff:

* `Controller.prototype.generateGet`: return a v4 signed url which allows
  access to an object
* `Client` support for downloading files
* command line tool to run all the requests
* command line tool to do a complete upload locally -- mainly as an integration test

## Method Signatures:
In all cases, `permissions` and `tags` are optional parameters.  The parameters
of all functions are validated through the use of Joi schemas.  These schemas
are partially specified in the method body and comprised of schemas stored in
`src/schemas.js`

### Controller
* `new Controller({region, runner, runnerOpts})`
* `Controller.prototype.initiateMultipartUpload({bucket, key, sha256, size, permissions}) -> uploadId`
* `Controller.prototype.generateMultipartRequest({bucket, key, uploadId, parts}) -> [{url, method, headers}]`
* `Controller.prototype.completeMultipartUplaod({bucket, key, etags, tags, uploadId}) -> 'ETAG_OF_OBJECT'`
* `Controller.prototype.abortMultipartUpload({bucket, key, uploadId}) -> void`
* `Controller.prototype.generateSinglepartRequest({bucket, key, sha256, size, tags, permissions}) -> {url, method, headers}`

### Runner
The public api of this method is the `.run()` method.  All other methods which
aren't prefixed with double underscores are OK to use externally but are not
supported
* `new Runner(agent, agentOpts)`
* `Runner.prototype.run({req, body, streamingOutput}) -> {body | bodyStream, headers, statusCode, statusMessage}`

### Client
The `partsize` parameter is the size of the multiple part of the upload in
bytes.  This value specifies how large each individual upload requets will be.
The `multisize` parameter is the size of file which will cause the method to
switch from single part upload to multipart upload.
* `new Client({runner, runnerOpts, partsize, multisize}`
* `Client.prototype.prepareUpload({filename, forceSP, forceMP, partsize}) -> {filename, sha256, size, parts: [] | undefined`
* `Client.prototype.runUpload(request, upload) -> ['ETAG_OF_EACH_REQUEST']`

## Command line tools
TODO: write the command line tool that does upload and download

## Examples
TODO: write some examples

## Hacking
This library has a suite of unit tests which can be run without credentials.

```
npm install .
npm test
```

There are integration tests which can be run, but require S3 credentails and
an existing bucket to test against.
```
export BUCKET=mybucket
export AWS_ACCESS_KEY_ID=myaccesskeyid
export AWS_SECRET_ACCESS_KEY=mytoken

npm test
```


