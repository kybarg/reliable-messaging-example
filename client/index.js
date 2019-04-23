const Queue = require('better-queue');
const request = require('request');
const { URL } = require('url');
const LoremIpsum = require('lorem-ipsum').LoremIpsum;
const host = 'http://localhost:3123'

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

const acknowledgementQueueOptions = {
  maxRetries: 100, // Retry task in queue {Number} times
  // maxTimeout: 1000, // Time to wait for the task to be resolved befor marking as faild
  retryDelay: 1000, // Delay betwee each task attempt
  filo: false,
}


const queueOptions = {
  ...acknowledgementQueueOptions,
  priority: function (input, cb) {
    let priority = 10
    if (input.url === '/documents' && input.method === 'GET') priority = 5;
    cb(null, priority);
  }
}

/**
 * Acknowledgement Queue
 */
const acknowledgementQueue = new Queue((input, callback) => {

  /**
   * Simulating local error (hardware, conection)
   */
  const hasError = Math.random() >= 0.8;
  if (hasError) {
    return callback('Random local error')
  }

  const {
    url,
    method,
    body,
    headers,
  } = input

  request({
    uri: new URL(url, host),
    method,
    body,
    json: true,
    headers,
  }, (err, res, body) => {
    const statusCode = res ? res.statusCode : null;

    if (err) {
      return callback(err)
    }

    // if request is succesfull but with error code
    if (statusCode && (statusCode === 429 || (500 <= statusCode && statusCode < 600))) {
      return callback(body)
    }

    callback(null, body)
  })
}, acknowledgementQueueOptions);


/**
 * Requests queue
 */
const requestsQueue = new Queue((input, callback) => {
  const {
    url,
    method,
    // body,
  } = input

  /**
   * Simulating local error (hardware, conection)
   */
  const hasError = Math.random() >= 0.8;
  if (hasError) {
    return callback('Random local error')
  }

  request({
    uri: new URL(url, host),
    method,
    // body,
    json: true,
    headers: {
      'X-Reliable': true,
    }
  }, (err, res, body) => {
    const statusCode = res ? res.statusCode : null;

    if (err) {
      return callback(err)
    }

    // if request is succesfull but with error code
    if (statusCode && (statusCode === 429 || (500 <= statusCode && statusCode < 600))) {
      return callback(body)
    }

    const messageId = res.headers['x-messageid'];
    if (!messageId) return callback('No messageId');

    acknowledgementQueue.push({
      ...input,
      headers: {
        'X-Reliable': true,
        'X-MessageId': messageId
      },
    })
      .on('finish', (result) => { callback(null, result) })
      .on('failed', (error) => callback(error))
  })
}, queueOptions);

console.log('Starting delivery of 5 documents')

/**
 * Adding 5 documents to the queue
 */
for (let index = 0; index < 5; index++) {
  requestsQueue.push({
    url: '/documents',
    method: 'POST',
    body: {
      title: lorem.generateSentences(1),
      text: lorem.generateSentences(3),
    },
  })
    .on('finish', (result) => {
      console.log('Succesfully delivered document', result.id);

      if (result.id === 5) {

        // Viewing the result
        request({
          uri: new URL('/documents', host),
          method: 'GET',
          json: true,
        }, (err, res, body) => {
          console.log('documents', Object.keys(body));

          console.log('acknowledgementQueue stats', acknowledgementQueue.getStats());
          console.log('requestsQueue stats', requestsQueue.getStats());
        })
      }
    })
}

// requestsQueue.push({
//   url: '/documents',
//   method: 'GET',
// })
//   .on('finish', (result) => {
//     console.log('documents', Object.keys(result));

//     console.log('acknowledgementQueue stats', acknowledgementQueue.getStats());
//     console.log('requestsQueue stats', requestsQueue.getStats());
//   })
