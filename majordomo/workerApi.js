const zmq = require('zeromq');
const assert = require('assert');
const MDP = require('./mdp');

const HEARTBEAT_LIVENESS = 3; // 3-5 is reasonable

/**
Majordomo Protocol Client API, Java version Implements the MDP/Worker spec at
http://rfc.zeromq.org/spec:7.
*/
class WorkerAPI {
  /**
   * @param {String} broker
   * @param {String} service
   * @param {bool} verbose
   */
  constructor(broker, service, verbose) {
    this.identity = 'worker';
    this.worker;           // Socket to broker
    this.heartbeatAt;      // When to send HEARTBEAT
    this.liveness;         // How many attempts left
    this.heartbeat = 2500; // Heartbeat delay, msecs
    this.reconnect = 2500; // Reconnect delay, msecs

    // Internal state
    this.expectReply = false; // false only at start

    this.timeout = 2500;
    this.verbose;                            // Print activity to stdout

    // Return address, if any
    this.replyTo = ''; // ZFrame

    assert(broker != null);
    assert(service != null);
    this.broker = broker;
    this.service = service;
    this.verbose = verbose;
    this.reconnectToBroker();
  }

  /**
   * Send message to broker If no msg is provided, creates one internally
   *
   * @param {String} command // From MDP
   * @param {String} option
   * @param {Buffer[]} msg
   */
  sendToBroker(command, option = null, msg = null) {
    msg = msg != null ? msg.slice(0) : [];

    // Stack protocol envelope to start of message
    if (option != null)
      msg.unshift(option);

    msg.unshift(command);
    msg.unshift(MDP.W_WORKER);
    msg.unshift('');

    if (this.verbose) {
      console.log(`I: sending ${command} to broker\n`);
      console.log(msg.map((frame) => frame ? frame.toString() : ''));
    }

    this.worker.send(msg);
  }

  /**
   * Connect or reconnect to broker
   */
  reconnectToBroker() {
    if (this.worker != null) {
      this.worker.close();
    }
    this.worker = zmq.socket('dealer');
    this.worker.identity = this.identity;
    this.worker.connect(this.broker);
    if (this.verbose)
      console.log(`I: connecting to broker at ${this.broker}\n`);

    // Register service with broker
    this.sendToBroker(MDP.W_READY, this.service, null);

    // If liveness hits zero, queue is considered disconnected
    this.liveness = HEARTBEAT_LIVENESS;
    this.heartbeatAt = (new Date()).getTime() + this.heartbeat;
  }

  /**
   * Send reply, if any, to broker and wait for next request.
   *
   * @param {Buffer[]} reply
   */
  receive(reply) {

    // Format and send the reply if we were provided one
    assert(reply != null || !this.expectReply);

    if (reply != null) {
      assert(this.replyTo != null);
      reply.unshift(this.replyTo);
      this.sendToBroker(MDP.W_REPLY, null, reply);
    }
    this.expectReply = true;

    setInterval(() => {
      // Poll socket for a reply, with timeout
      const msg = this.worker.read();
      if (msg) {
        if (this.verbose) {
          console.log("I: received message from broker: \n");
          console.log(msg.map((frame) => frame.toString()));
        }
        this.liveness = HEARTBEAT_LIVENESS;
        // Don't try to handle errors, just assert noisily
        assert(msg != null && msg.size() >= 3);

        const empty = msg.pop();
        assert(empty.getData().length == 0);

        const header = msg.pop();
        assert(MDP.W_WORKER === header.toString());

        const command = msg.pop();
        if (MDP.W_REQUEST === command.toString()) {
          // We should pop and save as many addresses as there are
          // up to a null part, but for now, just save one
          this.replyTo = msg.shift();
          return msg; // We have a request to process
        }
        else if (MDP.W_HEARTBEAT === command.toString()) {
          // Do nothing for heartbeats
        }
        else if (MDP.W_DISCONNECT === command.toString()) {
          this.reconnectToBroker();
        }
        else {
          console.log("E: invalid input message: \n");
          console.log(msg.map((frame) => frame.toString()));
        }
      }
      else if (--this.liveness == 0) {
        if (verbose)
          console.log("W: disconnected from broker - retrying\n");
        setTimeout(() => {
          this.reconnectToBroker();
        }, this.reconnect)
      }

      // Send HEARTBEAT if it's time
      if ((new Date()).getTime() > this.heartbeatAt) {
        sendToBroker(MDP.W_HEARTBEAT, null, null);
        this.heartbeatAt = (new Date()).getTime() + heartbeat;
      }
    }, this.timeout)

    if (Thread.currentThread().isInterrupted())
      console.log("W: interrupt received, killing worker\n");
    return null;
  }

  // ==============   getters and setters =================

  /**
   * @returns {Number}
   */
  getHeartbeat() {
    return heartbeat;
  }

  /**
   * @param {Number} heartbeat
   */
  setHeartbeat(heartbeat) {
    this.heartbeat = heartbeat;
  }

  /**
   * @returns {Number}
   */
  getReconnect() {
    return reconnect;
  }

  /**
   * @param {Number} reconnect
   */
  setReconnect(reconnect) {
    this.reconnect = reconnect;
  }

}

module.exports = WorkerAPI
