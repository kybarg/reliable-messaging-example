const zmq = require('zeromq');
const assert = require('assert');
const MDP = require('./mdp');

/**
* Majordomo Protocol Client API, asynchronous Java version. Implements the
* MDP/Worker spec at http://rfc.zeromq.org/spec:7.
*/
class ClientAPI {

  /**
   *
   * @param {String} broker
   * @param {Boolean} verbose
   */
  constructor(broker, verbose) {
    this.client = null; // socket
    this.timeout = 2500;
    this.broker = broker;
    this.verbose = verbose;
    this.identity = 'client';
    this.reconnectToBroker();
  }

  getTimeout() {
    return this.timeout;
  }

  /**
   *
   * @param {Number} timeout - time in milliseconds
   */
  setTimeout(timeout) {
    this.timeout = timeout;
  }

  /**
   * Connect or reconnect to broker
   */
  reconnectToBroker() {
    if (this.client != null) {
      this.client.close()
    }
    this.client = zmq.socket('dealer');
    this.client.identity = this.identity;
    this.client.connect(this.broker);
    if (this.verbose)
      console.log("I: connecting to broker at %s…\n", this.broker);
  }

  /**
   * Returns the reply message or NULL if there was no reply. Does not attempt
   * to recover from a broker failure, this is not possible without storing
   * all unanswered requests and resending them all…
   *
   * @param {Buffer[]} msg
   */
  recv() {
    let reply = null;

    const msg = this.client.read();
    if (!msg)
      return null; // Interrupted

    if (this.verbose) {
      console.log("I: received reply: \n");
      console.log(msg.map((frame) => frame.toString()));
    }
    // Don't try to handle errors, just assert noisily
    assert(msg.size() >= 4);

    const empty = msg.pop();
    assert(empty.getData().length == 0);

    const header = msg.pop();
    assert(MDP.C_CLIENT === header.toString());

    const replyService = msg.pop();

    reply = msg;

    return reply;
  }

  /**
   * Send request to broker and get reply by hook or crook Takes ownership of
   * request message and destroys it when sent.
   *
   * @param {String} service
   * @param {Buffer[]} request
   */
  send(service, request) {
    assert(request != null);

    // Prefix request with protocol frames
    // Frame 0: empty (REQ emulation)
    // Frame 1: "MDPCxy" (six bytes, MDP/Client x.y)
    // Frame 2: Service name (printable string)
    request.unshift(service);
    request.unshift(MDP.C_CLIENT);
    request.unshift('');
    if (this.verbose) {
      console.log('I: send request to %s service: \n', service);
      console.log(request.map((frame) => frame.toString()));
    }
    return this.client.send(request);
  }
}

module.exports = ClientAPI
