const zmq = require('zeromq');
const Queue = require('better-queue');
const dealer = zmq.socket('router');

/**
 * Queue code
 */

// const workerQueue = new Queue((message, callback) => {
//   dealer.send([args[0], args[1], '', 'OK'], 0, () => {

//   })
// })


/**
 * Server code
 */

dealer.identity = 'dealer';
dealer.bindSync('tcp://127.0.0.1:5556')

// setTimeout(() => {
//   dealer.send('READY')
// }, 3000)

dealer.on('message', function () {
  const args = Array.apply(null, arguments)
  console.log('message: %s', args[3].toString());

  console.log('args[0]', args[0].toString());
  console.log('args[1]', args[1].toString());
  console.log('args[2]', args[2].toString());
  console.log('args[3]', args[3].toString());

  dealer.send([args[0], args[1], '', 'OK'])
})

process.on('SIGINT', function () {
  dealer.close();
  process.exit();
});
