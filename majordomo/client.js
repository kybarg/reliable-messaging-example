const ClientAPI = require('./clientApi')


const clientSession = new ClientAPI('tcp://localhost:5555', true);
let count = 0

for (count = 0; count < 3; count++) {
    const request = [];
    request.push("Hello world");
    clientSession.send("echo", request);
}
for (count = 0; count < 3; count++) {
    const reply = clientSession.recv();
    console.log('reply', reply)
    if (reply != null)
        console.log('got replay')
    else break; // Interrupt or failure
}

console.log("%d requests/replies processed\n", count);
// process.exit()
