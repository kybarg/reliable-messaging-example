const Queue = require('better-queue');
const request = require('request');
const { URL } = require('url');
const LoremIpsum = require('lorem-ipsum').LoremIpsum;
const ClientAPI = require('./clientApi');
const uuid = require('uuid/v4');

/**
 * Constants
 */
const SERVER_ENDPOINT = 'tcp://127.0.0.1:5555';
const client = new ClientAPI(SERVER_ENDPOINT, 'client-159')

// Random text generator
const lorem = new LoremIpsum({
  sentencesPerParagraph: {
    max: 8,
    min: 4
  },
  wordsPerSentence: {
    max: 16,
    min: 4
  }
});


// let i = 0
// setInterval(() => {
//   client.sendMessage(`request ${i}`)
//     .on('response', (msg) => {
//       console.log('msg', msg.map((arg) => arg.toString()))
//       // i += 1;
//     })
//     .on('error', (error) => {
//       console.log(error);
//     })
//     i += 1;
// }, 1000);


const queueOptions = {
  // maxRetries: 1000000, // Retry task in queue {Number} times
  // maxTimeout: 0, // Time to wait for the task to be resolved befor marking as faild
  retryDelay: 1000, // Delay betwee each task attempt
  // filo: false,
  concurrent: 1, // One document a time
  // precondition: function (cb) {
  //   if (client.isLive()) {
  //     return cb(null, true);
  //   }
  //   return cb(null, false);
  // },
  // preconditionRetryTimeout: 2000,
}

/**
 * Requests queue
 */
const requestsQueue = new Queue((input, callback) => {
  const {
    id,
    ...message
  } = input

  /**
   * Simulating local error (hardware, conection)
   */
  // const hasError = Math.random() >= 0.8;
  // if (hasError) {
  //   return callback('Random local error')
  // }

  client.sendMessage([id, message])
    .then((msg) => {
      callback(null, msg);
    })
    .catch((error) => {
      console.log('fdgsdfgsdgsdfgsdfgsdfgsgfsdgsdfgsfgsdgsgsgf', error)
      callback(error);
    })

    // .on('response', (msg) => {
    //   callback(null, msg);
    // })
    // .on('error', (error) => {
    //   console.log('fdgsdfgsdgsdfgsdfgsdfgsgfsdgsdfgsfgsdgsgsgf', error)
    //   callback(error);
    // })

}, queueOptions);

console.log('Starting delivery of 5 documents')

/**
 * Adding 5 documents to the queue
 */
for (let index = 0; index < 10; index++) {
  requestsQueue.push({
    id: `${process.pid}-${uuid()}-${index}`,
    url: '/documents',
    method: 'POST',
    body: {
      id: index,
      title: lorem.generateSentences(1),
      text: lorem.generateSentences(3),
    },
  })
    .on('finish', (result) => {
      console.log(result)
      console.log('Succesfully delivered document', result[1]);
    })
    .on('error', (result) => {
      console.log('eeeeeeeeeeeeeErrrrrrrrrrrrrrrrooooooooooooorrrrrrrrrrrrr');
    })
}
