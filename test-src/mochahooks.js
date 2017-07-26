process.on('unhandledRejection', err => {
  throw err;
});

var fs = require('fs');

if (!fs.existsSync(__dirname + '/../bigfile')) {
  var spawn = require('child_process').spawnSync;
  spawn('dd', ['if=/dev/urandom', 'of=' + __dirname + '/../bigfile', 'bs=' + 1024*1024, 'count=1'], {
    stdio: [0, 1, 2],
  });
}
