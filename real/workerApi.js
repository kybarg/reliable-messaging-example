const zmq = require('zeromq');
const MDP = require('./mdp');

const backend = zmq.socket('router');

backend.bindSync("tcp://127.0.0.1:5555"); //  For workers

backend.on('message', function () {
  // handleHeartbeating();
  const msg = Array.apply(null, arguments);

  if (msg.length === 0)
    return;

  //  Validate control message, or return reply to client
  if (msg.length == 3) {
    const frame = msg[2];
    const data = frame.toString();

    if (data === MDP.W_READY || data === MDP.W_HEARTBEAT) {
      backend.send(msg);
    } else {
      console.log('E: invalid message from worker');
      console.log('msg', msg.map((arg) => arg.toString()));
    }
  } else if (msg.length == 5) {
    const frame = msg[2];
    const data = frame.toString();
    if (data === MDP.W_REQUEST) {
      /**
       * Simulating local error (hardware, conection)
       */
      const hasError = Math.random() >= 0.8;
      if (!hasError) return;

      // console.log('E: sending repsonse in 5s');
      console.log('msg', msg.map((arg) => arg.toString()));
      backend.send([msg[0], msg[1], MDP.W_REPLY, msg[3]]);
      // }, 5000);
    } else {
      console.log('E: invalid message from worker');
      console.log('msg', msg.map((arg) => arg.toString()));
    }
  } else {
    console.log('msg from backend', msg.map((arg) => arg.toString()), (new Date()).getSeconds())
    // backend.send(msg);
  }
})
