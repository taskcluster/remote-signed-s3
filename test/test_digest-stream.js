const { DigestStream } = require('../lib/digest-stream');
const fs = require('fs');
const crypto = require('crypto');
const assume = require('assume');

const tmpfile = 'delete-this-file-if-you-see-it';

describe('Digest Stream', () => {
  let x = [
    new DigestStream(),
    new DigestStream({algorithm: 'md5'}),
    new DigestStream({format: 'base64'}),
    new DigestStream({algorithm: 'sha1', format: 'base64'}),
  ];
  let z = 1;
  for (let y of x) {
    it('should hash a stream correctly ' + z++, done => {
      let readstream = fs.createReadStream(__dirname + '/../package.json');
      let writestream = fs.createWriteStream(tmpfile);
      let digeststream = new DigestStream();

      writestream.on('finish', () => {
        try {
          let orig = fs.readFileSync(__dirname + '/../package.json');
          let copy = fs.readFileSync(tmpfile);

          assume(copy.toString('base64')).equals(orig.toString('base64'));
          assume(digeststream.hash).equals(crypto
            .createHash(digeststream._algorithm)
            .update(fs.readFileSync(tmpfile))
            .digest(digeststream._format));

          fs.unlinkSync(tmpfile);

          done();
        } catch (err) {
          done(err);
        }
      });

      readstream.pipe(digeststream).pipe(writestream);
    });
  }
});
