process.on('unhandledRejection', err => {
  throw err;
});

var fs = require('fs');

if (!fs.existsSync(__dirname + '/../bigfile')) {
  var spawn = require('child_process').spawnSync;
  var outcome = spawn('dd', ['if=/dev/urandom', 'of=' + __dirname + '/../bigfile', 'bs=' + 1024*1024, 'count=20'], {
    stdio: [0, 1, 2],
  });
  if (outcome.status !== 0) {
    throw new Error('failed to create sample file for tests');
  }
}
