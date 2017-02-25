const crypto = require('crypto');
const fs = require('fs');
const assume = require('assume');
const sinon = require('sinon');
const Runner = require('../lib/runner');

const assertReject = require('./utils').assertReject;
const run = new Runner().run;

const httpbin = 'https://httpbin.org/';

describe('Request Runner', () => {
  it('should be able to make a basic call', async () => {
    let result = await run({
      url: httpbin + 'ip',
      method: 'GET',
      headers: {},
    });
  });
  
  it('should work with a lower case method', async () => {
    let result = await run({
      url: httpbin + 'ip',
      method: 'get',
      headers: {},
    });
  });

  it('should send headers correctly', async () => {
    let result = await run({
      url: httpbin + 'headers',
      method: 'get',
      headers: {
        'test-header': 'hi'
      },
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
        url: httpbin + 'status/' + status,
        method: 'get',
        headers: {},
      });

      assume(result).has.property('statusCode', status);
    });

  }

  for (let method of ['post', 'put']) {
    it(`should be able to ${method} data from a string body`, async () => {
      let result = await run({
        url: httpbin + method,
        method: method,
        headers: {
          key: 'value',
        },
      }, 'abody' + method);
      
      let body = JSON.parse(result.body);
      assume(body).has.property('data', 'abody' + method);
    });
    
    it(`should be able to ${method} data from a Buffer body`, async () => {
      let result = await run({
        url: httpbin + method,
        method: method,
        headers: {
          key: 'value',
        },
      }, Buffer.from('abody' + method));
      
      let body = JSON.parse(result.body);
      assume(body).has.property('data', 'abody' + method);
    });
  
    it(`should be able to ${method} data from a streaming body`, async () => {
      let result = await run({
        url: httpbin + method,
        method: method,
        headers: {
          key: 'value',
        },
      }, fs.createReadStream(__dirname + '/../package.json'));
      
      let body = JSON.parse(result.body);
      assume(body).has.property('data',
        fs.readFileSync(__dirname + '/../package.json').toString());
    });
  }
});
