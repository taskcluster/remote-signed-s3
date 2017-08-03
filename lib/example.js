#!/usr/bin/env node
'use strict';

const fs = require('mz/fs');
const crypto = require('crypto');
const path = require('path');
require('source-map-support/register');

const Client = require('./client');
const {Controller} = require('./controller');

/**
 * This is an example implementation of this library.  It is designed to
 * demonstrate how this library might be implemented on a client and server.
 *
 * NOTE: There is no state maintained about which upload is currently
 * happening, so the server always assumes only one upload is ever happening
 * concurrently.  Maintaining this state is a concern of the implementor of
 * this library.  The UploadID is what should be used here.  This is done
 * so that this example serves as the minimal example needed to exercise this
 * library
 */


// This class represents roughly what an api client would look like to the
// consumer.  Think of the functions other than the constructor as being
// remotely called methods.  In a CI implementation, this would be the API
// Client.
class ServerAPI {
  constructor(opts) {
    this.controller = new Controller(/*{region: 'eu-central-1'}*/);
    this.key = undefined;
    this.bucket = undefined;
    this.uploadId = undefined;
    this.parts = undefined;
    // NOTE: It's important to remember that while the tags are set for single
    // part uploads during the creation, the tags sent with the createArtifact
    // method need to be saved in state for use when running the
    // completeArtifact method, since multipart uploads set their tags *after*
    // they're finished
    // TODO: Verify the above....
    this.tags = undefined;
  }

  async createArtifact(opts) {
    this.bucket = process.env.S3_BUCKET || 'example-remote-s3';
    this.key = path.basename(opts.filename);
    this.tags = opts.tags || {};
    this.parts = opts.parts;
    this.uploadId = undefined;

    if (opts.parts) {
      this.uploadId = await this.controller.initiateMultipartUpload({
        bucket: this.bucket,
        key: this.key,
        permissions: {
          acl: 'public-read',
        },
        sha256: opts.sha256,
        size: opts.size,
      });
    }

    let requests;
    if (opts.parts) {
      requests = await this.controller.generateMultipartRequest({
        bucket: this.bucket,
        key: this.key,
        uploadId: this.uploadId,
        sha256: opts.sha256,
        size: opts.size,
        parts: opts.parts,
      });
    } else {
      requests = await this.controller.generateSinglepartRequest({
        bucket: this.bucket,
        key: this.key,
        sha256: opts.sha256,
        size: opts.size,
        permissions: {
          acl: 'public-read',
        },
        tags: {
          owner: 'example.js',
        },
      });
    }

    // See!  we can send JSON-serializable requests to the client
    return JSON.stringify(requests);
  }

  async completeArtifact(etags) {
    // Only multi-part uploads need to do something specific here
    if (this.parts) {
      let options = {
        bucket: this.bucket,
        key: this.key,
        uploadId: this.uploadId,
        etags: etags,
      };
      if (this.tags) {
        options.tags = this.tags;
      }
      await this.controller.completeMultipartUpload(options);
    }
  }

  async cancelArtifact(opts) {
    
  }
}

// This class represents what would be implemented to call into the ServerAPI
// remote proceedure calls.  In a CI implement, this would be the process which
// wishes to upload files
class Worker {
  constructor(server) {
    this.client = new Client();
    this.server = server;
  }

  async uploadFile(filename) {
    let fileinfo;
    let result;

    // Let's figure out the file parts first
    try {
      fileinfo = await this.client.prepareUpload({filename, forceMP: process.env.FORCEMP});
    } catch (err) {
      console.log('Error computing information about file');
      throw err;
    }

    let requests = JSON.parse(await this.server.createArtifact(fileinfo));

    // Now let's run the requests that the ServerAPI has given us
    try {
      result = await this.client.runUpload(requests, fileinfo);
    } catch (err) {
      console.log('Error uploading file');
      await this.server.cancelArtifact();
      throw err;
    }

    console.log(JSON.stringify(result, null, 2));
    console.log(result.responses[0].body)

    let outcome = await this.server.completeArtifact(result.etags);
  }
}

async function main(files) {
  let serverApi = new ServerAPI();
  let worker = new Worker(serverApi);

  for (let file of files) {
    let stats = await fs.stat(file);
    console.log(`Uploading ${file} ${stats.size} bytes`);
    await worker.uploadFile(file);
    console.log(`Completed ${file}`);
  }


}


main(process.argv.slice(2)).then(() => {}, err => { console.log(err.stack||err)});
