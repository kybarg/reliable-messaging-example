const zmq = require('zeromq');
const uuid = require('uuid');
const EventEmitter = require('events');
const eventToPromise = require('event-to-promise');
// const Envelope = require('./envelope')
const MDP = require('./mdp');

const REQUEST_TIMEOUT = 2500; //  msecs, (> 1000!)
// const REQUEST_RETRIES = 3; //  Before we abandon

const HEARTBEAT_LIVENESS = 3; //  3-5 is reasonable
const HEARTBEAT_INTERVAL = 1000; //  msecs

class Client {
  constructor(endpoint, identity) {
    this.identity = identity;
    this.endpoint = endpoint;

    this.eventEmitter = new EventEmitter();
    this.socket = null;
    this.expectedReply = null;
    this.requestInterval;

    //  If liveness hits zero, queue is considered disconnected
    this.liveness = HEARTBEAT_LIVENESS;

    console.log('I: connecting\n');
    this.createSocket();
    this.startHeartbeat();

    this.eventEmitter.on('received', (reply) => {
      console.log('reply-------------------------------', reply)
      clearInterval(this.requestInterval);
      this.expectedReply = null;
    })
  }

  createSocket() {
    // If client exists, remove and create new one
    if (this.socket !== null) {
      this.socket._flushWrites();
      this.socket._flushReads();
      this.socket.disconnect(this.endpoint);
      this.socket.close();
      this.socket = null;
    }

    this.socket = zmq.socket('dealer');
    this.socket.identity = this.identity;
    // this.socket.monitor(1, 0);
    this.socket.connect(this.endpoint);
    this.socket.send(MDP.W_READY);
    // this.socket.on('connect', this.handleConnect.bind(this));
    this.socket.on('message', this.handleMessage.bind(this));
  }

  // handleConnect() {
  //   //  Tell queue we're ready for work
  //   console.log('I: conencted\n');
  //   this.socket.send(MDP.W_READY);
  //   this.liveness = HEARTBEAT_LIVENESS;
  //   this.interval = INTERVAL_INIT;
  // }

  handleMessage() {
    const msg = Array.apply(null, arguments);

    if (msg.length === 0)
      return;

    if (msg.length === 1) {
      const frame = msg[0];
      const data = frame.toString();

      if (data === MDP.W_HEARTBEAT || data === MDP.W_READY) {
        this.liveness = HEARTBEAT_LIVENESS;
      } else {
        console.log('E: invalid message\n');
        console.log(msg.map((arg) => arg.toString()));
      }
    } else if (msg.length === 2) {
      const type = msg[0].toString();
      const reply = msg[1].toString();
      if (type === MDP.W_REPLY && reply === this.expectedReply) {
        this.eventEmitter.emit('received', reply);
        this.eventEmitter.emit(reply, reply);
      } else {
        console.log('E: invalid reply\n');
        console.log(msg.map((arg) => arg.toString()));
      }
    }
  }

  startHeartbeat() {
    setInterval(() => {
      this.liveness -= 1

      if (this.liveness > 0) {
        console.log('I: sent heartbeat at %s\n', (new Date()).getDate());
        this.socket.send(MDP.W_HEARTBEAT);
      } else if (this.liveness === 0) {
        console.log('I: heartbeat failure, can\'t reach queue\n');
        console.log('I: reconnecting\n');
        this.createSocket();
      }
    }, HEARTBEAT_INTERVAL);
  }

  sendMessage(id, message) {
    if (!this.expectedReply) {
      this.expectedReply = id;

      if (Array.isArray(message)) {
        message.unshift(id);
        message.unshift(MDP.W_REQUEST);
      } else {
        message = [MDP.W_REQUEST, id, message];
      }

      // this.requestTime = (new Date).getTime() + REQUEST_TIMEOUT;
      if (this.isLive()) {
        this.socket.send(message);

      }
      this.requestInterval = setInterval(() => {
        if (this.isLive()) this.socket.send(message);
      }, REQUEST_TIMEOUT)

      console.log('this.expectedReply-------------------------------', this.expectedReply)
      return eventToPromise(this.eventEmitter, id);
    }

    console.log('this.expectedReply-------------------------------', this.expectedReply)
    return Promise.reject('Already processing task');
  }

  isLive() {
    return this.liveness > 0;
  }

  isBusy() {
    return this.expectedReply !== null;
  }
}

module.exports = Client
