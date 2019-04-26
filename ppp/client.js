const zmq = require('zeromq');
const MDP = require('./mdp');

const REQUEST_TIMEOUT = 2500;                  //  msecs, (> 1000!)
const REQUEST_RETRIES = 3;                     //  Before we abandon
const SERVER_ENDPOINT = "tcp://127.0.0.1:5555";
const identity = 'cleint' + process.pid;

console.log("I: connecting to server");
let client = zmq.socket('req');
client.identity = identity;
client.connect(SERVER_ENDPOINT);

let sequence = 0;
let retriesLeft = REQUEST_RETRIES;
let expect_reply = 0;

setInterval(() => {
  if (retriesLeft > 0) {
    const request = String(++sequence);
    client.send(request);
    console.log('I: sent request: %s\n', request)
    expect_reply = 1;

    setTimeout(() => {
      if (--retriesLeft == 0) {
        console.log(
          "E: server seems to be offline, abandoning\n"
        );
        return;
      }
      else {
        console.log("W: no response from server, retrying\n");
        //  Old socket is confused; close it and open a new one
        client.close();
        console.log("I: reconnecting to server\n");
        client = zmq.socket('req');
        client.identity = identity;
        client.connect(SERVER_ENDPOINT);
        //  Send request again, on new socket
        client.send(request);
      }
    }, REQUEST_TIMEOUT)

  }
}, 1000)

client.on('message', function (reply) {
  // const reply = Array.apply(null, arguments);

  if (!reply)
    return;

  if (Number(reply) === sequence) {
    console.log("I: server replied OK (%s)\n", reply);
    retriesLeft = REQUEST_RETRIES;
    expect_reply = 0;
  }
  else console.log("E: malformed reply from server: %s\n", reply);
});
