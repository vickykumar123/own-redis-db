// Redis server implementing RESP protocol with expiry support
import * as net from "net";
import {parseRESPCommand, encodeError} from "./parser";
import {RedisCommands} from "./commands";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

const redisCommands = new RedisCommands();

const server: net.Server = net.createServer((socket: net.Socket) => {
  // Handle connection
  socket.on("data", async (data: Buffer) => {
    try {
      const parsedCommand = parseRESPCommand(data);

      if (!parsedCommand) {
        socket.write(encodeError("ERR Invalid command format"));
        return;
      }

      const {command, args} = parsedCommand;
      await redisCommands.executeCommand(command, args, socket);
    } catch (error) {
      console.error("Error processing command:", error);
      socket.write(encodeError("ERR Internal server error"));
    }
  });
});

server.listen(6379, "127.0.0.1");
