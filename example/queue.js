const Queue = require('better-queue');
// const ClientAPI = require('./clientApi');
const uuid = require('uuid/v4');
const SQLStore = require('better-queue-sql');
// const SQLStore = require('better-queue-sqlite');
const Client = require('./client')


const client = new Client('tcp://127.0.0.1:5555', 'client');


/**
 * Constants
 */
const SERVER_ENDPOINT = 'tcp://127.0.0.1:5555';
// const client = new ClientAPI(SERVER_ENDPOINT, 'client-159')


const queueOptions = {
  // filo: false,
  // retryDelay: 10000, // Delay betwee each task attempt
  concurrent: 1, // One document a time
  batchsize: 1,
  store: new SQLStore({ path: './queue.db.sqlite' }),
  priority: function (name, cb) {
    cb(null, -(new Date()).getTime());
  },
  precondition: function(cb) {
    cb(null, (client.isLive() && !client.isBusy()));
  },
  preconditionRetryTimeout: 5*1000,
}

/**
 * Requests queue
 */
const requestsQueue = new Queue((input, callback) => {
  const {
    id,
    message,
  } = input

  client.sendMessage(id, JSON.stringify(message))
    .then((msg) => {
      callback(null, msg);
    })
    .catch((error) => {
      callback(error);
    })
}, queueOptions);

requestsQueue.on('task_finish', function (taskId, result, stats) {
  // taskId = 1, result: 3, stats = { elapsed: <time taken> }
  // taskId = 2, result: 5, stats = { elapsed: <time taken> }
  console.log('success taskId', taskId)
  console.log('success result', result)
  console.log('\n')
})
requestsQueue.on('task_failed', function (taskId, err, stats) {
  // Handle error, stats = { elapsed: <time taken> }
  console.log('failed taskId', taskId)
  console.log('failed err', err)
  console.log('\n')
})


function getRandomArbitrary(min, max) {
  return Math.random() * (max - min) + min;
}

const order = (id) => ({
  OrderId: id,
  // CoinType: 'BTC',
  // CoinAmount: 0,
  // CoinPrice: getRandomArbitrary(3000, 3500),
  // CustomerId: uuid(),
  // FiatType: 'USD',
  // FiatRate: getRandomArbitrary(0.5, 1),
  // FiatAmount: 0,
  // orderType: 'Buy',
  // Status: 'BUYING',
  // WalletId: uuid(),
})


for (let index = 0; index < 10; index++) {
  requestsQueue.push({
    id: `${process.pid}-${uuid()}-${index}`,
    message: order(index),
  })
}



// module.exports = requestsQueue;
