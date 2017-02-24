let crypto = require('crypto');
let fs = require('fs');
let assume = require('assume');
let sinon = require('sinon');
let { run } = require('../');

const httpbin = 'https://httpbin.org/';

async function assertThrow(promise) {
  return new Promise(async (res, rej) => {
    try {
      await promise;
      rej(new Error('Should have thrown'));
    } catch (err) {
      res(err);
    }
  });
}

describe('assertThrown', () => {
  it('rejects for resolved promise', () => {
    return assertThrow(assertThrow(Promise.resolve()));
  });

  it('resolves for rejected promise', () => {
    return assertThrow(Promise.reject());
  });
});

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
    return assertThrow(run({
      url: httpbin + 'headers',
      method: 'get',
      headers: {
        'test-header': 'hi'
      },
    }, 'HA'));
  });

  for (let status of [100, 199, 300, 400, 500]) {
    it(`should throw an error with a ${status} HTTP Status Code`, () => {
      return assertThrow(run({
        url: httpbin + status,
        method: 'get',
        headers: {},
      }));
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
