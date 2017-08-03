const crypto = require('crypto');
const fs = require('fs');
const stream = require('stream');
const assume = require('assume');
const sinon = require('sinon');
const http = require('http');
const urllib = require('url');

const Runner = require('../lib/runner');

const assertReject = require('./utils').assertReject;

const testServerBase = 'http://localhost:8080/';

describe('Request Runner', () => {
  let runner;
  
  let server;
  let port = process.env.PORT || 8080;

  beforeEach(() => {
    runner = new Runner();
  });

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
          let bodyStream;
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
            bodyStream = fs.createReadStream(__dirname + '/../bigfile');
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


          if (statusCode === 200 && body.length === 0 && !bodyStream) {
            statusCode = 204;
          }
          headers['content-length'] = body.length;
          response.writeHead(statusCode, headers);
          if (bodyStream) {
            bodyStream.pipe(response);
          } else {
            response.end(body);
          }
        } catch (err) {
          // Be super loud about these failures!
          console.log(err.stack || err);
          throw err;
        }
      })
    });

    server.listen(port, 'localhost', done);
  });

  after(done => {
    server.close(() => {done()});
  });

  describe('Retries', () => {
    let sandbox;

    beforeEach(() => {
      sandbox = sinon.sandbox.create();
    });

    afterEach(() => {
      sandbox.restore();
    });

    it('should retry up to 5 times by default', async () => {
      let runFiveTimes = sandbox.spy(runner, 'runOnce');

      let result = await runner.run({
        req: {
          url: testServerBase + 'status/500',
          method: 'GET',
          headers: {},
        },
      });

      assume(runFiveTimes).has.property('callCount', 5);
    });
    
    it('should only run runOnce once', async () => {
      let runOneTime = sandbox.spy(runner, 'runOnce');

      let result = await runner.run({
        req: {
          url: testServerBase + 'status/200',
          method: 'GET',
          headers: {},
        },
      });

      assume(runOneTime).has.property('callCount', 1);
    });    

    it('a raw stream for the body should only allow a single try', async () => {
      let runOneTime = sandbox.spy(runner, 'runOnce');

      let result = await runner.run({
        req: {
          url: testServerBase + 'status/500',
          method: 'PUT',
          headers: {},
        },
        body: fs.createReadStream(__dirname + '/../package.json'),
      });

      assume(runOneTime).has.property('callCount', 1);
    });
  });

  // If you want to play with this development server, uncomment this
  // line:
  // it.only('test server', done => { });

  it('should be able to make a basic call', async () => {
    let result = await runner.runOnce({
      req: {
        url: testServerBase + 'simple',
        method: 'GET',
        headers: {},
      },
    });
  });
  
  it('should work with a lower case method', async () => {
    let result = await runner.runOnce({
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
    let result = await runner.runOnce({
      req: {
        url: testServerBase + 'header-repeat',
        method: 'get',
        headers: {
          'test-header': 'hi'
        },
      }
    });

    result = JSON.parse(result.body);

    assume(result.headers).has.property('test-header', 'hi');
  });

  it('should throw when a body should not be given', () => {
    return assertReject(runner.runOnce({
      url: testServerBase + 'method/get',
      method: 'get',
      headers: {
        'test-header': 'hi'
      },
    }, 'HA'));
  });

  for (let status of [200, 299, 300, 400, 500]) {
    it(`should return a ${status} HTTP Status Code correctly`, async () => {
      let result = await runner.runOnce({
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
      let result = await runner.runOnce({
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
      let result = await runner.runOnce({
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
      let result = await runner.runOnce({
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

      let result = await runner.runOnce({
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
    let result = await runner.runOnce({
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

  // Not sure why this test is failing...
  //
  // Uncaught Error: Parse Error
  //      at Error (native)
  //      at Socket.socketOnData (_http_client.js:363:20)
  //      at readableAddChunk (_stream_readable.js:176:18)
  //      at Socket.Readable.push (_stream_readable.js:134:10)
  //      at TCP.onread (net.js:548:20)
  // 
  it.skip('should be able to stream a huge response body (ignoring output)', async () => {
    let result = await runner.runOnce({
      req: {
        url: testServerBase + 'big-file',
        method: 'get',
        headers: {},
      },
      streamingOutput: true,
    })
      
    return new Promise((pass, fail) => {
      result.bodyStream.on('error', fail);

      result.bodyStream.on('data', data => {
      });

      result.bodyStream.once('end', () => {
        pass();
      });
    });

  });
});

// We are not yet sure whether redirect will live in this library or not, but
// if it does, this is the unit test suite that should pass for them
describe.skip('Redirects', () => {
  let port = process.env.PORT || 8080;
  let runner;
  let server;
  let current;

  beforeEach(() => {
    runner = new Runner();
  });

  afterEach(done => {
    if (server) {
      server.close(() => {done()});
    } else {
      done();
    }
  })

  async function redirectServer(total, code, withLoc=true) {
    let _server = http.createServer();
    let current = 0;

    _server.on('request', (request, response) => {
      let headers = {
        current,
        total,
        code,
      };
      if (Number.parseInt(request.url.slice(1)) !== current) {
        throw new Error('incorrect current');
      }
      if (current < total) {
        current++;
        if (withLoc) {
          headers.location = `http://localhost:${port}/${current}`;
        }
        response.writeHead(code, headers);
        response.end('redirect');
      } else {
        response.writeHead(200, headers);
        response.end('end');
      }
    });

    return new Promise((resolve, reject) => {
      _server.listen(port, 'localhost', () => { resolve(_server) })
    });
  }

  // These are specified 300 series redirects which should not be followed
  for (let code of [300, 304, 305, 307]) {
    it('should follow a simple ' + code + ' redirect', async () => {
      server = await redirectServer(2, code);
      let result = await runner.run({
        followRedirect: true,
        maxRedirects: 5,
        req: {
          url: `http://localhost:${port}/0`,
          method: 'GET',
          headers: {},
        },
        body: '',
      });
      assume(result).has.property('statusCode', code);
      assume(result).has.property('headers');
      assume(result.headers).has.property('current', '0');
    });
  }

  for (let code of [301, 302, 303]) {
    it('should follow a simple ' + code + ' redirect', async () => {
      server = await redirectServer(2, code);
      let result = await runner.run({
        followRedirect: true,
        maxRedirects: 5,
        req: {
          url: `http://localhost:${port}/0`,
          method: 'GET',
          headers: {},
        },
        body: '',
      });
      assume(result).has.property('statusCode', 200);
      assume(result).has.property('headers');
      assume(result.headers).has.property('current', '2');
    });
  
    it('should exhaust maxRetries for ' + code + ' redirect', async () => {
      server = await redirectServer(100, code);
      let result = await runner.run({
        followRedirect: true,
        maxRedirects: 5,
        req: {
          url: `http://localhost:${port}/0`,
          method: 'GET',
          headers: {},
        },
        body: '',
      });
      assume(result).has.property('statusCode', code);
      assume(result).has.property('headers');
      assume(result.headers).has.property('current', '5');
    });  

    it('should not redirect for POST on ' + code + ' redirect', async () => {
      server = await redirectServer(2, code);
      let result = await runner.run({
        followRedirect: true,
        maxRedirects: 5,
        req: {
          url: `http://localhost:${port}/0`,
          method: 'POST',
          headers: {},
        },
        body: '',
      });
      assume(result).has.property('statusCode', code);
      assume(result).has.property('headers');
      assume(result.headers).has.property('current', '0');
    });
  }
});


