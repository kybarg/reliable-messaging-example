const zmq = require('zeromq');
const MDP = require('./mdp');

const REQUEST_TIMEOUT = 2500; //  msecs, (> 1000!)
const REQUEST_RETRIES = 3; //  Before we abandon

const HEARTBEAT_LIVENESS = 3; //  3-5 is reasonable
const HEARTBEAT_INTERVAL = 1000; //  msecs
const INTERVAL_INIT = 1000; //  Initial reconnect
const INTERVAL_MAX = 32000; //  After exponential backoff

class ClientAPI {
  constructor(endpoint, identity) {
    this.identity = identity;
    this.endpoint = endpoint;

    this.retriesLeft = REQUEST_RETRIES;
    this.requestTime = 0;
    this.expectReply = false;
    this.client = null;

    this.reconnectionTimeout;

    this.reply = null; // Promise

    //  If liveness hits zero, queue is considered disconnected
    this.liveness = HEARTBEAT_LIVENESS;
    this.interval = INTERVAL_INIT;

    this.clientSocket();
    this.startHeartbeat();
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
    this.client.send(MDP.W_READY);
    this.liveness = HEARTBEAT_LIVENESS;
    clearTimeout(this.reconnectionTimeout);
    this.interval = INTERVAL_INIT;
  }

  handleMessage() {
    //  Get message
    //  - 3-part envelope + content -> request
    //  - 1-part HEARTBEAT -> heartbeat
    const msg = Array.apply(null, arguments);
    console.log('msg', msg.map((arg) => arg.toString()))
    if (msg.length === 0)
      return;

    if (msg.length == 3) {
      console.log('I: normal reply\n');
      console.log(msg.map((arg) => arg.toString()))

      this.liveness = HEARTBEAT_LIVENESS;
    } else {
      //  When we get a heartbeat message from the queue, it
      //  means the queue was (recently) alive, so reset our
      //  liveness indicator:

      if (msg.length === 1) {
        const frame = msg[0];
        const frameData = frame.toString();

        if (MDP.W_HEARTBEAT === frameData || MDP.W_READY === frameData) {
          this.liveness = HEARTBEAT_LIVENESS;
        } else {
          console.log('E: invalid message\n');
          console.log( msg.map((arg) => arg.toString()));
        }
      } else if(msg.length === 2) {
        const frame = msg[0];
        const frameData = frame.toString();
        if (MDP.W_REPLY === frameData) {
          console.log('response: ', msg.map((arg) => arg.toString()));
        }
      } else {
        console.log('E: invalid message\n');
        console.log(msg.map((arg) => arg.toString()));

        // this.reply.resolve(msg.map((arg) => arg.toString()))
      }
    }

    // if (this.expectReply) {
    //   if (!message)
    //     return;

    //   if (Number(message.toString()) === sequence) {
    //     console.log('I: server replied OK (%s)\n', message);
    //     this.retriesLeft = REQUEST_RETRIES;
    //     this.expectReply = false;
    //   }
    //   else console.log('E: malformed reply from server: %s\n', reply);
    // }
  }

  startHeartbeat() {
    setInterval(() => {
      this.liveness -= 1
      if (this.liveness > 0) {
        const now = (new Date()).getTime();
        this.heartbeatAt = now + HEARTBEAT_INTERVAL;
        // console.log('I: sent heartbeat\n');
        this.client.send(MDP.W_HEARTBEAT, () => {
          const msg = Array.apply(null, arguments);
          console.log('heartbeat callback', msg.map((arg) => arg.toString()))
        });
      } else if (this.liveness === 0) {
        console.log('I: heartbeat failure, can\'t reach queue\n');
        console.log('I: reconnecting in %s msec\n', this.interval);
        // this.client._flushReads();
        // this.client._flushWrites();
        this.client.close();
        this.client = null;

        this.reconnectionTimeout = setTimeout(() => {
          if (this.interval < INTERVAL_MAX)
            this.interval *= 2;
          this.clientSocket();
        }, this.interval)
      }
    }, HEARTBEAT_INTERVAL);
  }

  sendMessage(request) {
    if (!this.reply) {
      if(Array.isArray(request)) {
        request.unshift(MDP.W_REQUEST);
      } else {
        request = [MDP.W_REQUEST, request];
      }

      this.client.send(request, function () {
        const msg = Array.apply(null, arguments);
        console.log('message callback', msg.map((arg) => arg.toString()))
      });
      this.requestTime = (new Date).getTime() + REQUEST_TIMEOUT;
      this.expectReply = 1;
    }
  }

  isLive() {
    return this.liveness > 0;
  }
}

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
