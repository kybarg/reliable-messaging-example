const zmq = require('zeromq');
const sock = zmq.socket('push');

sock.bindSync('tcp://127.0.0.1:5512');
console.log('Producer bound to port 5512');

let i = 0;
setInterval(function () {
  console.log('sending work:', i);
  sock.send(i);

  i += 1;
}, 1500);
