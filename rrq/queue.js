const zmq = require('zeromq');
const frontend = zmq.socket('router');
const backend = zmq.socket('router');

frontend.identity = 'frontend';
backend.identity = 'backend';

const workers = [];

backend.on('message', function () {
  const args = Array.apply(null, arguments);
  workers.push(args[1])

  if (args[1] != 'READY') {
    console.log('message from server: %s', args[3])

    // console.log('args[0]', args[0].toString());
    // console.log('args[1]', args[1].toString());
    // console.log('args[2]', args[2].toString());
    // console.log('args[3]', args[3].toString());

    // frontend.send('dfgsdfg');
    frontend.send([args[1], args[2], args[3]]);
  }
})

backend.connect('tcp://127.0.0.1:5556')

frontend.bind('tcp://127.0.0.1:5555', function (err) {
  if (err) throw err;

  frontend.on('message', function () {
    const args = Array.apply(null, arguments);
    console.log('message from client: %s', args[2])

    // What if no workers are available? Delay till one is ready.
    // This is because I don't know the equivalent of zmq_poll
    // in Node.js zeromq, which is basically an event loop itself.
    // I start an interval so that the message is eventually sent. \
    // Maybe there is a better way.
    frontend.send([args[0], '', 'received']);
    backend.send(['dealer', args[0], '', args[2]]);

    // const interval = setInterval(function () {
    //   if (workers.length > 0) {
    //     backend.send([workers.shift(), args[0], '', args[2]]);
    //     clearInterval(interval)
    //   }
    // }, 10);
  });
});


process.on('SIGINT', function () {
  frontend.close();
  backend.close();
  process.exit();
});
