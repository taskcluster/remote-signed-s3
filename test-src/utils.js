async function assertReject(promise) {
  return new Promise(async (res, rej) => {
    try {
      await promise;
      rej(new Error('Should have thrown'));
    } catch (err) {
      res(err);
    }
  });
}

describe('assertRejectn', () => {
  it('rejects for resolved promise', () => {
    return assertReject(assertReject(Promise.resolve()));
  });

  it('resolves for rejected promise', () => {
    return assertReject(Promise.reject());
  });
});

module.exports.assertReject = assertReject;
