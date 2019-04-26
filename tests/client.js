const zmq = require('zeromq');
const sock = zmq.socket('req');

sock.connect('tcp://127.0.0.1:5512');
console.log('Client bound to port 5512');

sock.send('READY', null, function () {
  const args = Array.apply(null, arguments);
  console.log(args);
});

for (let index = 0; index < 10; index++) {
  console.log('sending work:', index);
  sock.send(index, null, function () {
    const args = Array.apply(null, arguments);
    console.log(args);
  });
}

process.on('SIGINT', function () {
  sock.close();
  process.exit();
});
