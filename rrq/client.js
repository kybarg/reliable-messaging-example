const zmq = require('zeromq');
const client = zmq.socket('req');

client.identity = 'client';


Object.keys(zmq.events).forEach((key) => {
  client.on(zmq.events[key], function () {
    console.log('monitor: ', zmq.events[key]);
  });
})

client.connect('tcp://127.0.0.1:5555');
console.log('Client bound to port 5555');

// client.on('message', function () {
//   console.log('message:');
//   const args = Array.apply(null, arguments);
//   console.log(args.map((arg) => arg.toString()))
// });

client.on('error', function () {
  console.log('error:');
  const args = Array.apply(null, arguments);
  console.log(args.map((arg) => arg.toString()))
})

client.on('closed', function () {
  console.log('closed:');
  const args = Array.apply(null, arguments);
  console.log(args.map((arg) => arg.toString()))
})

let reqNbr = 0;
setInterval(() => {
  client.send(`Message ${reqNbr}`, null, function () {
    console.log('message callback')
    // const args = Array.apply(null, arguments);
    // console.log(args.map((arg) => arg.toString()))
  });
  reqNbr += 1;
}, 2000);

client.monitor(10, 1);




process.on('SIGINT', function () {
  client.close();
  process.exit();
});
