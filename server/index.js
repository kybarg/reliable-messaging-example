
const Queue = require('better-queue');
const express = require('express');
const bodyParser = require('body-parser');
const uuid = require('uuid/v4');
const port = 3123;

/**
 * Object fro storing message references
 */
const messages = {}

/**
 * Object for simulating document storage
 */
const documents = {}

/**
 * Simulating database queue
 */
const databaseQueue = new Queue((input, callback) => {
  setTimeout(() => {
    const id = Object.keys(documents).length + 1;
    const document = {
      ...input,
      id,
    }

    const hasError = Math.random() >= 0.5;
    if (hasError) {
      return callback('Database error');
    }

    // Writing document to database
    documents[id] = document;
    return callback(null, document)
  }, 1000)
}, { maxRetries: 100 })

/**
 * Simulating random API error 20%
 */

const randomError = (massage) => (req, res, next) => {
  const hasError = Math.random() >= 0.7;
  if (hasError) {
    console.log(massage);
    return res.status(500).json({ message: `Can't process request` });
  } else {
    next();
  }
}

const app = express();
app.use(bodyParser.json())

app.use((req, res, next) => {
  const isReliable = req.header('X-Reliable');
  const messageId = req.header('X-MessageId');

  if (isReliable) {
    console.log('REQUEST', req.method, req.url, messageId ? `messageId: ${messageId}` : '');
    if (messages[messageId]) console.log('messages[messageId]', messages[messageId])
  } else {
    console.log('REQUEST', req.method, req.url);
  }
  next();
});

/**
 * Executer error before reliable logic simulating server error
 */
app.use(randomError('Error before server started working on request'));

app.use((req, res, next) => {
  const isReliable = req.header('X-Reliable');
  if (!isReliable) {
    return next();
  }

  /**
   * Check request for `messageId`
   */
  const messageId = req.header('X-MessageId');
  if (messageId) {
    /**
     * Check if `messageId` is present in our messages store
     */

    if (messages.hasOwnProperty(messageId)) {
      const destination = messages[messageId];

      /**
       * If `messageId` is present in store and not `null`, redirect request to destination
       * Else return error of request being processing
       */
      if (destination) {
        if (destination === 'PROCESSING') {
          return res.status(500).json({ message: `Referense with messageId: ${messageId} is still processing` });
        }
        console.log('REDIRECTED', req.method, req.url)
        res.removeHeader('X-Reliable');
        // res.removeHeader('X-MessageId');
        return res.redirect(302, destination);
      } else {
        next();
      }
    } else {
      /**
       * Id `messageId` is not in store already respond with error
       */
      res.status(500).json({ message: `Referense with messageId: ${messageId} no longer exists` });
    }
  } else {
    const newMessageId = uuid();
    messages[newMessageId] = null;
    res.append('X-MessageId', newMessageId);
    res.end();
  }
});


/**
 * Executer error after reliable logic simulating request processign error
 */
app.use(randomError('Error during working on server'));

app.use((req, res, next) => {
  const messageId = req.header('X-MessageId');
  const destination = messages[messageId];
  if (messageId && !destination) messages[messageId] = 'PROCESSING';
  next();
});

app.get('/documents/:id', (req, res) => {
  /**
   * Reading document from database
   */
  const documentId = req.params.id;

  if (!documentId) {
    return res.status(500).end();
  }
  const document = documents[documentId];
  res.json(document);
});

app.post('/documents', (req, res) => {
  const messageId = req.header('X-MessageId');
  if (messageId) messages[messageId] = 'PROCESSING';

  databaseQueue.push(req.body)
    .on('finish', (result) => {
      /**
       * If `messageId` is there push redirection url to messages store
       */
      if (messageId) messages[messageId] = `/document/${result.id}`;
      res.json(result)
    });
});

app.get('/documents', (req, res) => {
  res.json(documents);
});


// setInterval(() => {
//   console.log('-------------------------------')
//   console.log('messages', messages)
//   console.log('documents', documents)
// }, 3000)

app.listen(port, () => console.log(`Example app listening on port ${port}!`));

