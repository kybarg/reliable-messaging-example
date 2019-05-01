const zmq = require('zeromq');
const util = require('util');
const EventEmitter = require('events');

const MDP = require('./mdp');

const REQUEST_TIMEOUT = 2500; //  msecs, (> 1000!)
const REQUEST_RETRIES = 3; //  Before we abandon

function waitForEvent(emitter) {
  return new Promise((resolve, reject) => {
    emitter.once('error', reject);
    emitter.once('response', resolve)
  })
}

class ClientAPI {
  constructor(endpoint, identity) {
    this.identity = identity;
    this.endpoint = endpoint;

    this.retriesLeft = REQUEST_RETRIES;

    this.responseInterval;

    this.expectReply = false;
    this.client = null;

    this.reconnectionTimeout;

    this.eventEmmiter = new EventEmitter();

    this.clientSocket();
  }

  clientSocket() {
    // If client exists, remove and create new one
    if (this.client !== null) {
      this.client.close();
      this.client = null;
    }

    this.client = zmq.socket('req');
    this.client.identity = this.identity;
    this.client.monitor(10, 0);
    console.log('I: connecting\n');
    this.client.connect(this.endpoint);
    this.client.on('connect', this.handleConnect.bind(this));
    this.client.on('message', this.handleMessage.bind(this));
  }

  handleConnect() {
    //  Tell queue we're ready for work
    console.log('I: conencted\n');
    // clearTimeout(this.reconnectionTimeout);
  }

  handleMessage() {
    //  Get message
    //  - 3-part envelope + content -> request
    //  - 1-part HEARTBEAT -> heartbeat
    const msg = Array.apply(null, arguments);
    if (msg.length === 0 || !this.expectReply)
      return;

    if (true) { //need to heck for message id here
      console.log('I: server replied OK (%s)\n');
      console.log(msg.map((arg) => arg.toString()))
      clearInterval(this.responseInterval)
      this.expectReply = false;
      this.retriesLeft = REQUEST_RETRIES;
      this.eventEmmiter.emit('response', msg.map((arg) => arg.toString()));
    } else {
      console.log('E: malformed reply from server: %s\n');
      console.log(msg.map((arg) => arg.toString()));
    }
  }

  sendMessage(request) {
    // Reject requests when transporter is busy sending request
    if (this.expectReply) {
      let error = 'Transporter is busy!';
      setTimeout(() => {
        this.eventEmmiter.emit('error', error);
      }, 1)
      return waitForEvent(this.eventEmmiter);
    }

    if (Array.isArray(request)) {
      request.unshift(MDP.W_REQUEST);
    } else {
      request = [MDP.W_REQUEST, request];
    }

    this.client.send(request);
    this.expectReply = true;

    this.responseInterval = setInterval(() => {
      this.retriesLeft -= 1;
      if (this.retriesLeft === 0) {
        // console.log('E: server seems to be offline, abandoning\n');
        // this.eventEmmiter.emit('error', 'Server seems to be offline, abandoning!');
        // this.expectReply = false;
        // clearInterval(this.responseInterval);
      } else {
        console.log('W: no response from server, retrying request %s\n', request);
        //  Old socket is confused; close it and open a new one
        console.log('I: reconnecting to server\n');
        this.clientSocket();
        //  Send request again, on new socket
        this.client.send(request);
        this.expectReply = true;
      }

    }, REQUEST_TIMEOUT);

    return waitForEvent(this.eventEmmiter);
  }

}

// util.inherits(ClientAPI, EE);

// const identity = 'cleint' + process.pid;

// console.log('I: connecting to server');

// let sequence = 0;
// let retriesLeft = REQUEST_RETRIES;
// let requestTime = 0;
// let expectReply = 0;
// let request = String(sequence);
// let client;

// const createClient = () => {
//   const client = zmq.socket('req');
//   client.identity = identity;
//   client.monitor(10, 0);
//   client.connect(SERVER_ENDPOINT);
//   client.on('message', function (reply) {
//     if (expectReply > 0) {
//       if (!reply)
//         return;

//       if (Number(reply.toString()) === sequence) {
//         console.log('I: server replied OK (%s)\n', reply);
//         retriesLeft = REQUEST_RETRIES;
//         expectReply = 0;
//         sequence += 1;
//         request = String(sequence);
//       }
//       else console.log('E: malformed reply from server: %s\n', reply);
//     }
//   });

//   client.on('connect', function () {
//     console.log('connected');
//   })

//   return client;
// }

// client = createClient();

// let messagesInterval;
// messagesInterval = setInterval(function () {
//   if (expectReply === 0) {
//     client.send(request);
//     requestTime = (new Date).getTime() + REQUEST_TIMEOUT;
//     expectReply = 1;
//   } else {
//     if (requestTime < (new Date).getTime()) {
//       retriesLeft -= 1
//       if (retriesLeft === 0) {
//         console.log('E: server seems to be offline, abandoning\n');
//         clearInterval(messagesInterval);
//         return;
//       }
//       else {
//         console.log('W: no response from server, retrying request %s\n', request);
//         client.close();
//         client = null;
//         //  Old socket is confused; close it and open a new one
//         console.log('I: reconnecting to server\n');
//         client = createClient();
//         //  Send request again, on new socket
//         client.send(request);
//         requestTime = (new Date).getTime() + REQUEST_TIMEOUT;
//         expectReply = 1;
//       }
//     }
//   }
// }, 1000)

module.exports = ClientAPI
