import * as net from "net";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

// Uncomment this block to pass the first stage
const server: net.Server = net.createServer((socket: net.Socket) => {
  // Handle connection
  socket.on("data", (data: Buffer) => {
    const command = data.toString().trim();
    socket.write("+PONG\r\n");
  });
  //   socket.write("+PONG\r\n");
  socket.end();
});
//
server.listen(6379, "127.0.0.1");
