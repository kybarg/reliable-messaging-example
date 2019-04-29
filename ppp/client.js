const zmq = require('zeromq');
const MDP = require('./mdp');

const REQUEST_TIMEOUT = 2500;                  //  msecs, (> 1000!)
const REQUEST_RETRIES = 3;                     //  Before we abandon
const SERVER_ENDPOINT = 'tcp://127.0.0.1:5555';
const identity = 'cleint' + process.pid;

console.log('I: connecting to server');

let sequence = 0;
let retriesLeft = REQUEST_RETRIES;
let requestTime = 0;
let expect_reply = 0;
let request = String(sequence);
let client;

const createClient = () => {
  const client = zmq.socket('req');
  client.identity = identity;
  client.monitor(10, 0);
  client.connect(SERVER_ENDPOINT);
  client.on('message', function (reply) {
    if (expect_reply > 0) {
      if (!reply)
        return;

      if (Number(reply.toString()) === sequence) {
        console.log("I: server replied OK (%s)\n", reply);
        retriesLeft = REQUEST_RETRIES;
        expect_reply = 0;
        sequence += 1;
        request = String(sequence);
      }
      else console.log("E: malformed reply from server: %s\n", reply);
    }
  });

  client.on('connect', function () {
    console.log('connected');
  })

  return client;
}

client = createClient();

let messagesInterval;
messagesInterval = setInterval(function () {
  if (expect_reply === 0) {
    client.send(request);
    requestTime = (new Date).getTime() + REQUEST_TIMEOUT;
    expect_reply = 1;
  } else {
    if (requestTime < (new Date).getTime()) {
      retriesLeft -= 1
      if (retriesLeft === 0) {
        console.log('E: server seems to be offline, abandoning\n');
        clearInterval(messagesInterval);
        return;
      }
      else {
        console.log('W: no response from server, retrying request %s\n', request);
        client.close();
        client = null;
        //  Old socket is confused; close it and open a new one
        console.log('I: reconnecting to server\n');
        client = createClient();
        //  Send request again, on new socket
        client.send(request);
        requestTime = (new Date).getTime() + REQUEST_TIMEOUT;
        expect_reply = 1;
      }
    }
  }
}, 1000)

