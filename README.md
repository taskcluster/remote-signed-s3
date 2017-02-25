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

The `Controller` class has the following methods:

* `new Controller(region, runner)`: Create a `Controller` instance for the
  region specified by `region`.  The `runner` parameter is a function with the
  signature `(request, body)` which must take a standard interchange format
  object and returns an object like `Runner.prototype.run`
* `Controller.prototype.initiateMultipartUpload(opts)`: initiate a multipart
  upload in S3.  The opts dictionary must provide the `bucket` and `key` to 
  store the file in as well as `sha256` which is the SHA256 digest of the full
  file and `size` which is the number of bytes in the full file.  This method
  returns the string of of the `uploadId` to use for following requests.
* `Controller.prototype.generateMultipartRequest(opts)`: generate a list of
  standard interchange format objects which can be run to upload the file.
  The `bucket`, `key` and `uploadId` parameters must be included.  The `parts`
  parameter is a list of objects like `{sha256: '<sha256>', size: 1234}`.
  Note that the ordering of the list implies the ordering of the parts.  This
  method returns a list of signed requests which should be run by the
  untrusted host to upload the file.  Note that the order of these requests
  is equivalent by the order of the `parts` list
* `Controller.prototype.completeMultipartUpload(opts)`: instruct S3 to commit
  the upload to S3.  This will make the file available immediately.  The `opts`
  parameters `bucket`, `key` and `uploadId` parameters must be provided. The 
  `etags` parameter is a list of etags returned by the S3 API for the upload
  of each part.  This method returns the `etag` of the completed upload.
* `Controller.prototype.abortMultipartUpload(opts)`: instruct S3 to abort
  the upload.  The `bucket`, `key` and `uploadId` parameters must be provided.
  This method returns nothing
* `Controller.prototype.generateSinglepartRequest(opts)`: Generate a single
  request in the standard interchange format to upload an object to S3 and
  return it.  The `bucket`, `key`, `sha256` and `size` parameters must be
  specified


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

The `Runner` class has the following methods:

* `new Runner(agent, agentOpts)`: create a new `Runner` instance.  The `agent`
  parameter is the `https.Agent` to use for all requests.  If the `agent`
  parameter is falsy, then the `agentOpts` parameter will be passed to the
  constructor like so `new https.Agent(agentOpts)`
* `Runner.prototype.run(request, body, noThrow)`: Run a request using a standard
  interchange format object `request`.  A `string`, `Buffer` or `Readable`
  stream as the `body` parameter will be used to determine obtain the request
  body.  Note that for `string` and `Buffer` bodies, the entire body must
  be stored in memory.  The `Readable` stream option is recommended for all
  but the smallest files

The `Runner.prototype.run` method returns an object which looks like this:
```javascript
{
  body: "<entire response body>",
  headers: {
    aresponse: "header"
  },
  statusCode: 200,
  statusMessage: "OK",
  requestHash: "<sha256>",
  requestSize: 1234,
  responseHash: "<sha256>",
  responseSize: 4567
}
```

TODO: Write some stuff:

* `Controller.prototype.generateGet`: return a v4 signed url which allows
  access to an object
* `UploadRunner`: nice wrapper of `Controller` and `Runner` which lets someone
  do a complete upload and automatically pick single or multi part
* `RequestRunner`: something to give a list of request to just run and report
  back on.  Should have logic to do concurrency for multipart uploads
* `Downloader`: A nice wrapper of the `Runner` which can be used to download
  objects a list of interchange objects
* command line tool to run all the requests
* command line tool to do a complete upload locally -- mainly as an integration test

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


