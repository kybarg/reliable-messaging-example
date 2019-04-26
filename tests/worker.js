const zmq = require('zeromq');
const sock = zmq.socket('pull');

sock.connect('tcp://127.0.0.1:5512');
console.log('Worker connected to port 5512');

sock.on('message', function(msg){
  console.log('work: %s', msg.toString());
});
