const crypto = require('crypto');
const fs = require('fs');
const stream = require('stream');
const assume = require('assume');
const sinon = require('sinon');

const Runner = require('../lib/runner');

const assertReject = require('./utils').assertReject;
const run = new Runner().run;

const httpbin = 'https://httpbin.org/';

describe('Request Runner', () => {
  it('should be able to make a basic call', async () => {
    let result = await run({
      req: {
        url: httpbin + 'ip',
        method: 'GET',
        headers: {},
      },
    });
  });
  
  it('should work with a lower case method', async () => {
    let result = await run({
      req: {
        url: httpbin + 'ip',
        method: 'get',
        headers: {},
      },
    });
  });

  it('should send headers correctly', async () => {
    let result = await run({
      req: {
        url: httpbin + 'headers',
        method: 'get',
        headers: {
          'test-header': 'hi'
        },
      }
    });
    result = JSON.parse(result.body);

    assume(result.headers).has.property('Test-Header', 'hi');
  });

  it('should throw when a body should not be given', () => {
    return assertReject(run({
      url: httpbin + 'headers',
      method: 'get',
      headers: {
        'test-header': 'hi'
      },
    }, 'HA'));
  });

  for (let status of [200, 299, 300, 400, 500]) {
    it(`should return a ${status} HTTP Status Code correctly`, async () => {
      let result = await run({
        req: {
          url: httpbin + 'status/' + status,
          method: 'get',
          headers: {},
        },
      });

      assume(result).has.property('statusCode', status);
    });

  }

  for (let method of ['post', 'put']) {
    it(`should be able to ${method} data from a string body`, async () => {
      let result = await run({
        req: {
          url: httpbin + method,
          method: method,
          headers: {
            key: 'value',
          },
        },
        body: 'abody' + method,
      });
      
      let body = JSON.parse(result.body);
      assume(body).has.property('data', 'abody' + method);
    });
    
    it(`should be able to ${method} data from a Buffer body`, async () => {
      let result = await run({
        req: {
          url: httpbin + method,
          method: method,
          headers: {
            key: 'value',
          },
        },
        body: Buffer.from('abody' + method),
      });
      
      let body = JSON.parse(result.body);
      assume(body).has.property('data', 'abody' + method);
    });

    it(`should be able to ${method} data from a streaming body passed in`, async () => {
      let result = await run({
        req: {
          url: httpbin + method,
          method: method,
          headers: {
            key: 'value',
          },
        },
        body: fs.createReadStream(__dirname + '/../package.json')
      });

      
      let body = JSON.parse(result.body);
      assume(body).has.property('data',
        fs.readFileSync(__dirname + '/../package.json').toString());
    });  

    it(`should be able to ${method} data from a streaming body from function`, async () => {
      let bodyFactory = () => {
        return fs.createReadStream(__dirname + '/../package.json');
      };

      let result = await run({
        req: {
          url: httpbin + method,
          method: method,
          headers: {
            key: 'value',
          },
        },
        body: bodyFactory,
      });

      
      let body = JSON.parse(result.body);
      assume(body).has.property('data',
        fs.readFileSync(__dirname + '/../package.json').toString());
    });
  }

  it('should be able to stream a response body', async () => {
    let result = await run({
      req: {
        url: httpbin + 'stream-bytes/10?seed=1234&chunk_size=1',
        method: 'get',
        headers: {},
      },
      streamingOutput: true,
    });

    assume(result).to.be.an('object');
    assume(result).to.have.property('bodyStream');
    //assume(result.bodyStream).to.be.an.instanceof(stream.Readable);
    assume(result).to.not.have.property('body');
    assume(result).to.have.property('requestHash');

    return new Promise((resolve, reject) => {
      let chunks = [];

      result.bodyStream.on('error', reject);

      result.bodyStream.on('data', chunk => {
        chunks.push(chunk);
      });

      result.bodyStream.on('end', () => {
        let body = Buffer.concat(chunks);
        if (body.toString('base64') === '93AB6fCVqxXEPA==') {
          resolve();
        } else {
          reject();
        }
      });
    });
  });
});
