const zmq = require('zeromq');
const MDP = require('./mdp');

const HEARTBEAT_LIVENESS = 3;     //  3-5 is reasonable
const HEARTBEAT_INTERVAL = 1000;  //  msecs
const INTERVAL_INIT = 1000;  //  Initial reconnect
const INTERVAL_MAX = 32000; //  After exponential backoff

//  Helper function that returns a new configured socket
//  connected to the Paranoid Pirate queue

let heartbeatInterval;
let reconnectionTimeout;

const startheartbeatInterval = () => setInterval(() => {
  const now = (new Date()).getTime();
  heartbeat_at = now + HEARTBEAT_INTERVAL;
  console.log('I: worker heartbeat\n');
  worker.send(MDP.W_HEARTBEAT);
}, HEARTBEAT_INTERVAL)


function worker_socket() {
  const worker = zmq.socket('dealer');
  worker.identity = 'worker' + process.pid;
  worker.monitor(10, 0);
  worker.connect('tcp://127.0.0.1:5556');

  worker.on('connect', function () {
    //  Tell queue we're ready for work
    console.log('I: worker ready\n');
    worker.send(MDP.W_READY);
    heartbeatInterval = startheartbeatInterval();
    clearTimeout(reconnectionTimeout);
  })

  return worker;
}

//  We have a single task, which implements the worker side of the
//  Paranoid Pirate Protocol (PPP). The interesting parts here are
//  the heartbeating, which lets the worker detect if the queue has
//  died, and vice-versa:

let worker = worker_socket();

//  If liveness hits zero, queue is considered disconnected
let liveness = HEARTBEAT_LIVENESS;
let interval = INTERVAL_INIT;

//  Send out heartbeats at regular intervals
let heartbeat_at = (new Date()).getTime() + HEARTBEAT_INTERVAL;

worker.on('message', function () {
  //  Get message
  //  - 3-part envelope + content -> request
  //  - 1-part HEARTBEAT -> heartbeat
  const msg = Array.apply(null, arguments);

  if (msg.length === 0)
    return;

  if (msg.length == 3) {
    console.log("I: normal reply\n");
    console.log(msg.map((arg) => arg.toString()))
    worker.send(msg);
    liveness = HEARTBEAT_LIVENESS;
  } else {
    //  When we get a heartbeat message from the queue, it
    //  means the queue was (recently) alive, so reset our
    //  liveness indicator:

    if (msg.length == 1) {
      const frame = msg[0];
      const frameData = frame.toString();

      if (MDP.W_HEARTBEAT === frameData) {
        liveness = HEARTBEAT_LIVENESS;
      } else {
        console.log('E: invalid message\n');
        console.log(msg.map((arg) => arg.toString()))
      }
    }
    else {
      console.log('E: invalid message\n');
      console.log(msg.map((arg) => arg.toString()))
    }
  }
})

const livenessInterval = setInterval(() => {
  //  If the queue hasn't sent us heartbeats in a while,
  //  destroy the socket and reconnect. This is the simplest
  //  most brutal way of discarding any messages we might have
  //  sent in the meantime.
  liveness -= 1
  if (liveness == 0) {
    console.log('W: heartbeat failure, can\'t reach queue\n');
    console.log('W: reconnecting in %s msec\n', interval);

    reconnectionTimeout = setTimeout(() => {
      if (interval < INTERVAL_MAX)
        interval *= 2;
      worker.close()
      clearInterval(heartbeatInterval);
      worker = worker_socket();
      liveness = HEARTBEAT_LIVENESS;
    }, interval)
  }
}, HEARTBEAT_INTERVAL);
