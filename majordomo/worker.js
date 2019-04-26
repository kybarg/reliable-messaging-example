const WorkerAPI = require('./workerApi');

const verbose = true;

const mdWorker = new WorkerAPI('tcp://localhost:5555', 'console.log', verbose);

// mdWorker.on('message', function(){
//   const request = Array.apply(null, arguments)

//   mdWorker.send
// })

// let reply = null;
// // while (true) {
//     const request = mdWorker.receive(reply);
//     reply = request; //  Echo is complex… :-)
// // }
