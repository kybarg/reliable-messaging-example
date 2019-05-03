const zmq = require('zeromq');
const low = require('lowdb')
const FileSync = require('lowdb/adapters/FileSync')
const MDP = require('./mdp');

const adapter = new FileSync('database.example.json')
const db = low(adapter)

// Exmaple database
db.defaults({ orders: [], messages: [] }).write()

let backend = zmq.socket('router');
backend.bindSync("tcp://127.0.0.1:5555"); //  For workers

backend.on('message', function () {
  const msg = Array.apply(null, arguments);

  // console.log('msg', msg.map((arg) => arg.toString()));

  if (msg.length === 0)
    return;

  if (msg.length === 2) {
    const frame = msg[1];
    const data = frame.toString();
    if (data === MDP.W_READY || data === MDP.W_HEARTBEAT) {
      backend.send(msg);
    } else {
      console.log('E: invalid message from worker');
      console.log('msg', msg.map((arg) => arg.toString()));
    }
  } else if (msg.length === 4) {

    // console.log('msg', msg.map((arg) => arg.toString()));

    // 20% request error
    let hasError = Math.random() >= 0.8;
    if (hasError) return

    const messageId = msg[2].toString();
    const message = db.get('messages').find({ id: messageId }).value();
    if (!message) {
        db.get('messages').push({ id: messageId, content: 'some' }).write();
        db.get('orders').push(JSON.parse(msg[3].toString())).write();
    }

    // 50% response error
    hasError = Math.random() >= 0.5;
    if (hasError) return

    backend.send([msg[0], MDP.W_REPLY, messageId]);
  } else {
    console.log('E: invalid message from worker');
    console.log('msg', msg.map((arg) => arg.toString()));
  }

  // //  Validate control message, or return reply to client
  // if (msg.length == 3) {
  //   const frame = msg[2];
  //   const data = frame.toString();

  //   if (data === MDP.W_READY || data === MDP.W_HEARTBEAT) {
  //     backend.send(msg);
  //   } else {
  //     console.log('E: invalid message from worker');
  //     console.log('msg', msg.map((arg) => arg.toString()));
  //   }
  // } else if (msg.length == 5) {
  //   const frame = msg[2];
  //   const data = frame.toString();
  //   if (data === MDP.W_REQUEST) {
  //     /**
  //      * Simulating local error (hardware, conection)
  //      */
  //     const hasError = Math.random() >= 0.8;
  //     if (!hasError) return;

  //     // console.log('E: sending repsonse in 5s');
  //     console.log('msg', msg.map((arg) => arg.toString()));
  //     backend.send([msg[0], msg[1], MDP.W_REPLY, msg[3]]);
  //     // }, 5000);
  //   } else {
  //     console.log('E: invalid message from worker');
  //     console.log('msg', msg.map((arg) => arg.toString()));
  //   }
  // } else {
  //   console.log('msg from backend', msg.map((arg) => arg.toString()), (new Date()).getSeconds())
  //   // backend.send(msg);
  // }
})


process.on('SIGINT', function () {
  // backend._flushReads();
  // backend._flushWrites();
  backend.unbindSync("tcp://127.0.0.1:5555");
  backend = null;
  process.exit();
});
