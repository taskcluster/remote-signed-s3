const crypto = require('crypto');
const fs = require('fs');
const stream = require('stream');
const assume = require('assume');
const sinon = require('sinon');
const http = require('http');
const urllib = require('url');

const Runner = require('../lib/runner');

const assertReject = require('./utils').assertReject;

const runner = new Runner();

const testServerBase = 'http://localhost:8080/';

describe('Request Runner', () => {
  
  let server;
  let port = process.env.PORT || 8080;

  before(done => {
    server = http.createServer();

    server.on('request', (request, response) => {
      let requestBody = [];
      let requestSize = 0;

      request.on('data', data => {
        requestBody.push(data);
        requestSize += data.length;
      });

      request.on('end', () => {
        requestBody = Buffer.concat(requestBody);
        try {
          let endpoint = urllib.parse(request.url).pathname.slice(1);
          let statusCode = 501;
          let headers = {};
          let body = '';
          let responseStream;


          if (/^simple\//.test(endpoint)) {
            statusCode = 200;
            body = 'Simple HTTP!';
          } else if (/^status\//.test(endpoint)) {
            statusCode = Number.parseInt(endpoint.split('/')[1]);
            body = statusCode.toString();
          } else if (/^file/.test(endpoint)) {
            statusCode = 200;
            body = fs.readFileSync(__dirname + '/../package.json');
          } else if (/^big-file/.test(endpoint)) {
            statusCode = 200;
            body = fs.readFileSync(__dirname + '/../bigfile');
          } else if (/^header-repeat/.test(endpoint)) {
            statusCode = 200;
            body = JSON.stringify({headers: request.headers});
          } else if (/^method\//.test(endpoint)) {
            statusCode = 200;
            let method = endpoint.split('/')[1].toUpperCase();
            if (request.method !== method) {
              statusCode = 400;
              body = 'You tried to ' + method + ' but actually ' + request.method + 'ed';
            } else {
              statusCode = 200;
              body = method;
            }
          } else if (/^echo-data\//.test(endpoint)) {
            statusCode = 200;
            let method = endpoint.split('/')[1];
            if (request.method === 'GET' || request.method === 'HEAD') {
              statusCode = 400;
              body = 'you cannot include data on a GET or HEAD';
            } else {
              statusCode = 200;
              body = JSON.stringify({requestBody: requestBody.toString(), requestSize});
            }
          } else {
            statusCode = 418;
          }


          if (statusCode === 200 && body.length === 0) {
            statusCode = 204;
          }
          headers['content-length'] = body.length;
          response.writeHead(statusCode, headers);
          response.end(body);
        } catch (err) {
          // Be super loud about these failures!
          console.log(err.stack || err);
          throw err;
        }
      })
    });

    server.listen(port, 'localhost', done);
  });

  // If you want to play with this development server, uncomment this
  // line:
  // it.only('test server', done => { });

  it('should be able to make a basic call', async () => {
    let result = await runner.run({
      req: {
        url: testServerBase + 'simple',
        method: 'GET',
        headers: {},
      },
    });
  });
  
  it('should work with a lower case method', async () => {
    let result = await runner.run({
      req: {
        url: testServerBase + 'method/get',
        method: 'get',
        headers: {},
      },
    });

    assume(result.statusCode).equals(200);
    assume(result.body.toString()).equals('GET');
  });

  it('should send headers correctly', async () => {
    let result = await runner.run({
      req: {
        url: testServerBase + 'header-repeat',
        method: 'get',
        headers: {
          'test-header': 'hi'
        },
      }
    });

    console.dir(result.body.toString());
    result = JSON.parse(result.body);

    assume(result.headers).has.property('test-header', 'hi');
  });

  it('should throw when a body should not be given', () => {
    return assertReject(runner.run({
      url: testServerBase + 'method/get',
      method: 'get',
      headers: {
        'test-header': 'hi'
      },
    }, 'HA'));
  });

  for (let status of [200, 299, 300, 400, 500]) {
    it(`should return a ${status} HTTP Status Code correctly`, async () => {
      let result = await runner.run({
        req: {
          url: testServerBase + 'status/' + status,
          method: 'get',
          headers: {},
        },
      });

      assume(result).has.property('statusCode', status);
    });

  }

  for (let method of ['post', 'put']) {
    it(`should be able to ${method} data from a string body`, async () => {
      let result = await runner.run({
        req: {
          url: testServerBase + 'echo-data/' + method,
          method: method,
          headers: {
            key: 'value',
          },
        },
        body: 'abody' + method,
      });
      
      assume(JSON.parse(result.body).requestBody).equals('abody' + method);
    });

    it(`should be able to ${method} data from a Buffer body`, async () => {
      let result = await runner.run({
        req: {
          url: testServerBase + 'echo-data/' + method,
          method: method,
          headers: {
            key: 'value',
          },
        },
        body: Buffer.from('abody' + method),
      });
      
      assume(JSON.parse(result.body).requestBody).equals('abody' + method);
    });
    
    it(`should be able to ${method} data from a streaming body passed in`, async () => {
      let result = await runner.run({
        req: {
          url: testServerBase + 'echo-data/' + method,
          method: method,
          headers: {
            key: 'value',
          },
        },
        body: fs.createReadStream(__dirname + '/../package.json')
      });
      
      assume(JSON.parse(result.body).requestBody)
        .equals(fs.readFileSync(__dirname + '/../package.json').toString());
    });
        
    it(`should be able to ${method} data from a streaming body from function`, async () => {
      let bodyFactory = () => {
        return fs.createReadStream(__dirname + '/../package.json');
      };

      let result = await runner.run({
        req: {
          url: testServerBase + 'echo-data/' + method,
          method: method,
          headers: {
            key: 'value',
          },
        },
        body: bodyFactory,
      });

      assume(JSON.parse(result.body).requestBody)
        .equals(fs.readFileSync(__dirname + '/../package.json').toString());
    });
  }

  it('should be able to stream a response body (checking output)', async () => {
    let result = await runner.run({
      req: {
        url: testServerBase + 'file',
        method: 'get',
        headers: {},
      },
      streamingOutput: true,
    });

    assume(result).to.be.an('object');
    assume(result).to.have.property('bodyStream');
    assume(result).to.not.have.property('body');

    return new Promise((resolve, reject) => {
      let chunks = [];

      result.bodyStream.on('error', reject);

      result.bodyStream.on('data', chunk => {
        chunks.push(chunk);
      });

      result.bodyStream.once('end', () => {
        let body = Buffer.concat(chunks);
        let expected = fs.readFileSync(__dirname + '/../package.json');
        if (body.toString() === expected.toString()) {
          resolve();
        } else {
          reject(new Error('Body does not match'));
        }
      });
    });
  });

  it('should be able to stream a huge response body (ignoring output)', done => {
    let result = runner.run({
      req: {
        url: testServerBase + 'file',
        method: 'get',
        headers: {},
      },
      streamingOutput: true,
    }).then(result => {
      let ws = fs.createWriteStream('/dev/null');
      result.bodyStream.once('error', done);
      ws.once('error', done);
      result.bodyStream.pipe(ws);
    }, done);;

  });
});
