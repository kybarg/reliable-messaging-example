const zmq = require('zeromq');
const MDP = require('./mdp');

//
// Paranoid Pirate queue
//

const HEARTBEAT_LIVENESS = 3;    //  3-5 is reasonable
const HEARTBEAT_INTERVAL = 1000; //  msecs

//  Here we define the worker class; a structure and a set of functions that
//  as constructor, destructor, and methods on worker objects:
class Worker {

  /**
   *
   * @param {Buffer} address
   */
  constructor(address) {
    this.address = address;  //  Address of worker
    this.identity = address.toString(); //  Printable identity
    this.expiry = (new Date()).getTime() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS; //  Expires at this time
  }

  /**
   * The ready method puts a worker to the end of the ready list:
   *
   * @param {Worker[]} workers
   */
  ready(workers) {
    for (let i = 0; i < workers.length; i += 1) {
      const worker = workers[i];
      if (this.identity === worker.identity) {
        workers.splice(i, 1);
      }
    }
    workers.push(this);
  }

  /**
   * The next method returns the next available worker address:
   *
   * @param {Worker[]} workers
   */
  static next(workers) {
    const worker = workers.shift();
    if (worker) {
      const frame = worker.address;
      return frame;
    }
    return
  }

  /**
   * The purge method looks for and kills expired workers. We hold workers
   * from oldest to most recent, so we stop at the first alive worker:
   *
   * @param {Worker[]} workers
   */
  static purge(workers) {

    for (let i = 0; i < workers.length; i += 1) {
      const worker = workers[i];
      if ((new Date()).getTime() < worker.expiry) {
        break;
      }
      workers.splice(i, 1);
    }
  }
};

//  The main task is an LRU queue with heartbeating on workers so we can
//  detect crashed or blocked worker tasks:

const frontend = zmq.socket('router');
const backend = zmq.socket('router');
frontend.bindSync("tcp://127.0.0.1:5555"); //  For clients
backend.bindSync("tcp://127.0.0.1:5556"); //  For workers

//  List of available workers
const workers = [];

//  Send out heartbeats at regular intervals
let heartbeat_at = (new Date()).getTime() + HEARTBEAT_INTERVAL;

backend.on('message', function () {
  handleHeartbeating();

  const msg = Array.apply(null, arguments);


  if (msg.length === 0)
    return;

  //  Any sign of life from worker means it's ready
  const address = msg.shift();
  const worker = new Worker(address);
  worker.ready(workers);

  console.log('msg.length ', msg.length)

  //  Validate control message, or return reply to client
  if (msg.length == 1) {
    const frame = msg[0];
    const data = frame.toString();
    console.log('W_HEARTBEAT ', data === MDP.W_HEARTBEAT)
    console.log('W_READY ', data === MDP.W_READY)

    if (data !== MDP.W_READY && data !== MDP.W_HEARTBEAT) {
      console.log('E: invalid message from worker');
      console.log('msg', msg.map((arg) => arg.toString()))
    }
  }
  else frontend.send(msg);
})

frontend.on('message', function () {
  // handleHeartbeating();
  const msg = Array.apply(null, arguments);

  // console.log('msg', msg.map((arg) => arg.toString()))

  if (msg.length === 0)
    return;

  // console.log(Worker.next(workers).toString())

  const workersAvailable = workers.length > 0;
  if (workersAvailable) {
    msg.unshift(Worker.next(workers));
    backend.send(msg);
  }
})


function handleHeartbeating() {
  //  We handle heartbeating after any socket activity. First we
  //  send heartbeats to any idle workers if it's time. Then we
  //  purge any dead workers:
  if ((new Date()).getTime() >= heartbeat_at) {
    workers.forEach((worker) => {
      // backend.send([worker.address, 128 + zmq.options._receiveMore]) // ZFrame.REUSE + ZFrame.MORE
      backend.send([worker.address, MDP.W_HEARTBEAT])
      // backend.send([MDP.W_HEARTBEAT])
    })
    const now = (new Date()).getTime();
    heartbeat_at = now + HEARTBEAT_INTERVAL;
  }
  Worker.purge(workers);
}
