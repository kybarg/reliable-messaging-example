const zmq = require('zeromq');
const assert = require('assert');
const MDP = require('./mdp');

// We'd normally pull these from config data
const INTERNAL_SERVICE_PREFIX = "mmi.";
const HEARTBEAT_LIVENESS = 3; // 3-5 is reasonable
const HEARTBEAT_INTERVAL = 2500; // msecs
const HEARTBEAT_EXPIRY = HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS;

// ---------------------------------------------------------------------
/**
 * This defines a single service.
 */
class Service {
  /**
   *
   * @param {String} name - Service name
   */
  constructor(name) {
    this.name = name;
    this.requests = []; // new ArrayDeque<ZMsg>(); List of client requests
    this.waiting = []; // new ArrayDeque<Worker>(); List of waiting workers
  }
}

// ---------------------------------------------------------------------
/**
 * This defines one worker, idle or active.
 */
class Worker {
  /**
   *
   * @param {String} identity - Identity of worker
   * @param {Buffer} address  - Address frame to route to
   */
  constructor(identity, address) {
    this.address = address;
    this.identity = identity;
    this.expiry = (new Date()).getTime() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS; // Expires at unless heartbeat
  }
}

/**
*  Majordomo Protocol broker
*  A minimal implementation of http://rfc.zeromq.org/spec:7 and spec:8
*/
class Broker {

  /**
   * Initialize broker state.
   *
   * @param {Boolean} verbose
   */
  constructor(verbose) {
    this.verbose = verbose || false;
    this.services = {}; // known services // new HashMap<String, Service>();
    this.workers = {}; // known workers // new HashMap<String, Worker>();
    this.waiting = []; // idle workers // new ArrayDeque<Worker>();
    this.heartbeatAt = (new Date()).getTime() + HEARTBEAT_INTERVAL;
    this.socket = zmq.socket('router');
    this.socket.on('message', this.mediate.bind(this))
  }

  // ---------------------------------------------------------------------

  /**
   * Main broker work happens here
   *
   * @param {Buffer[]} msg
   */
  mediate() {
    const msg = Array.apply(null, arguments);


      if (!msg) {
        this.destroy();
        return;
      }

      if (this.verbose) {
        console.log('I: received message:');
        console.log(msg.map((frame) => frame.toString()));
        console.log('\n');
      }

      const sender = msg.pop();
      const empty = msg.pop();
      const header = msg.pop();

      if (MDP.C_CLIENT === header.toString()) {
        this.processClient(sender, msg);
      }
      else if (MDP.W_WORKER === header.toString())
        this.processWorker(sender, msg);
      else {
        console.log('E: invalid message:');
        console.log(msg.map((frame) => frame.toString()));
        console.log('\n');
      }
      this.purgeWorkers();
      this.sendHeartbeats();
  }

  /**
   * Disconnect all workers, destroy context.
   */
  destroy() {
    const deleteList = Object.keys(this.workers).map((worker) => new Worker(worker));

    deleteList.forEach((worker) => {
      this.deleteWorker(worker, true);
    })
  }

  /**
   * Process a request coming from a client.
   *
   * @param {String} sender
   * @param {Array} msg
   */
  processClient(sender, msg) {
    assert(msg.length >= 2); // Service name + body
    const serviceFrame = msg.pop();
    // Set reply return address to client sender
    msg.unshift(sender);
    if (serviceFrame.toString().indexOf(INTERNAL_SERVICE_PREFIX) === 0)
      this.serviceInternal(serviceFrame, msg);
    else this.dispatch(this.requireService(serviceFrame), msg);
  }

  /**
   * Process message sent to us by a worker.
   *
   * @param {Buffer} sender
   * @param {Buffer[]} msg
   */
  processWorker(sender, msg) {
    assert(msg.length >= 1); // At least, command

    const command = msg.pop();

    const workerReady = this.workers.hasOwnProperty(sender.toString());

    const worker = this.requireWorker(sender);

    if (MDP.W_READY === command.toString()) {
      // Not first command in session || Reserved service name
      if (workerReady || sender.toString().indexOf(INTERNAL_SERVICE_PREFIX) === 0)
        deleteWorker(worker, true);
      else {
        // Attach worker to service and mark as idle
        const serviceFrame = msg.pop();
        worker.service = this.requireService(serviceFrame);
        this.workerWaiting(worker);
      }
    }
    else if (MDP.W_REPLY === command.toString()) {
      if (workerReady) {
        // Remove & save client return envelope and insert the
        // protocol header and service name, then rewrap envelope.
        const client = msg.shift();
        msg.unshift(worker.service.name);
        msg.unshift(MDP.C_CLIENT);
        msg.unshift(client);
        msg.send(socket);
        this.workerWaiting(worker);
      }
      else {
        this.deleteWorker(worker, true);
      }
    }
    else if (MDP.W_HEARTBEAT === command.toString()) {
      if (workerReady) {
        worker.expiry = (new Date()).getTime() + HEARTBEAT_EXPIRY;
      }
      else {
        this.deleteWorker(worker, true);
      }
    }
    else if (MDP.W_DISCONNECT === command.toString())
      this.deleteWorker(worker, false);
    else {
      console.log('E: invalid message:');
      console.log(msg.map((frame) => frame.toString()));
      console.log('\n');
    }
  }

  /**
   * Deletes worker from all data structures, and destroys worker.
   *
   * @param {Worker} worker
   * @param {Boolean} disconnect
   */
  deleteWorker(worker, disconnect) {
    assert(worker != null);
    if (disconnect) {
      this.sendToWorker(worker, MDP.W_DISCONNECT, null, null);
    }
    if (worker.service != null)
      delete worker.service.waiting[worker];
    delete workers[worker.identity]; //CHnaged here
    delete worker.address;
  }

  /**
   * Finds the worker (creates if necessary).
   *
   * @param {Buffer} address
   */
  requireWorker(address) {
    assert(address != null);
    const identity = address.toString();
    let worker = this.workers[identity];
    if (worker == null) {
      worker = new Worker(identity, address);
      this.workers[identity] = worker;
      if (this.verbose)
        console.log("I: registering new worker: %s\n", identity);
    }
    return worker;
  }

  /**
   * Locates the service (creates if necessary).
   *
   * @param {Buffer} serviceFrame
   * @return {Service}
   */
  requireService(serviceFrame) {
    assert(serviceFrame != null);
    const name = serviceFrame.toString();
    let service = this.services[name];
    if (service == null) {
      service = new Service(name);
      this.services[name] = service;
    }
    return service;
  }

  /**
   * Bind broker to endpoint, can call this multiple times. We use a single
   * socket for both clients and workers.
   *
   * @param {String} endpoint
   */
  bind(endpoint) {
    this.socket.bindSync(endpoint);
    console.log("I: MDP broker/0.1.1 is active at %s\n", endpoint);
  }

  /**
   * Handle internal service according to 8/MMI specification
   *
   * @param {Buffer} serviceFrame
   * @param {Array} msg
   */
  serviceInternal(serviceFrame, msg) {
    const returnCode = "501";
    if ("mmi.service" === serviceFrame.toString()) {
      const name = msg.slice(-1).pop().toString();
      returnCode = services.hasOwnProperty(name) ? "200" : "400";
    }
    msg.slice(-1).pop().reset(returnCode.getBytes(ZMQ.CHARSET));
    // Remove & save client return envelope and insert the
    // protocol header and service name, then rewrap envelope.
    const client = msg.shift();
    msg.unshift(serviceFrame);
    msg.unshift(MDP.C_CLIENT);
    msg.unshift(client);
    msg.send(socket);
  }

  /**
   * Send heartbeats to idle workers if it's time
   */
  sendHeartbeats() {
    // Send heartbeats to idle workers if it's time
    if ((new Date()).getTime() >= this.heartbeatAt) {
      this.waiting.forEach((worker) => {
        this.sendToWorker(worker, MDP.W_HEARTBEAT, null, null);
      })
      this.heartbeatAt = (new Date()).getTime() + HEARTBEAT_INTERVAL;
    }
  }

  /**
   * Look for & kill expired workers. Workers are oldest to most recent, so we
   * stop at the first alive worker.
   */
  purgeWorkers() {
    let w = this.waiting[0];
    while (w && w.expiry < (new Date()).getTime()) {
      console.log("I: deleting expired worker: %s\n", w.identity);
      this.deleteWorker(this.waiting.shift(), false);
      w = this.waiting[0];
    }
  }

  /**
   * This worker is now waiting for work.
   *
   * @param {Worker} worker
   */
  workerWaiting(worker) {
    // Queue to broker and service waiting lists
    this.waiting.push(worker);
    worker.service.waiting.push(worker);
    worker.expiry = (new Date()).getTime() + HEARTBEAT_EXPIRY;
    this.dispatch(worker.service, null);
  }

  /**
   * Dispatch requests to waiting workers as possible
   *
   * @param {Service} service
   * @param {Buffer[]} msg
   */
  dispatch(service, msg) {
    assert(service != null);
    if (msg != null)// Queue message if any
      service.requests.push(msg);
    this.purgeWorkers();
    while (service.waiting.length > 0 && service.requests > 0) {
      msg = service.requests.pop();
      const worker = service.waiting.pop();
      delete this.waiting[worker];
      this.sendToWorker(worker, MDP.W_REQUEST, null, msg);
    }
  }

  /**
   * Send message to worker. If message is provided, sends that message. Does
   * not destroy the message, this is the caller's job.
   *
   * @param {Worker} worker
   * @param {MDP} command
   * @param {String} option
   * @param {Array} msgp
   */
  sendToWorker(worker, command, option, msgp) {

    const msg = msgp == null ? [] : msgp;

    // Stack protocol envelope to start of message
    if (option != null)
      msg.unshift(option);
    msg.unshift(command);
    msg.unshift(MDP.W_WORKER);

    // Stack routing envelope to start of message
    msg.unshift(worker.address);
    if (this.verbose) {
      console.log("I: sending %s to worker\n", command);
      console.log(msg.map((frame) => frame.toString()));
    }
    msg.send(this.socket);
  }
}


const broker = new Broker(true)
broker.bind("tcp://127.0.0.1:5555");
// module.exports = new Broker
