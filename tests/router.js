const zmq = require('zeromq');
const sock = zmq.socket('router');

sock.bindSync('tcp://127.0.0.1:5512');
console.log('Router bound to port 5512');

sock.on('message', function () {
  const args = Array.apply(null, arguments)
  console.log('work: %s', args[2].toString());

  // if (args[2].toString() != 'READY') {
    sock.send(args);
  // }

});

process.on('SIGINT', function () {
  sock.close();
  process.exit();
});
