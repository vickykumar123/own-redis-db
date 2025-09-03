// Simple in-memory key-value store
interface IkvStore {
  value: string;
  expiry?: number;
}
const kvStore = new Map<string, IkvStore>();

import * as net from "net";
import {
  parseRESPCommand,
  encodeBulkString,
  encodeSimpleString,
  encodeError,
} from "./parser";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

const server: net.Server = net.createServer((socket: net.Socket) => {
  // Handle connection
  socket.on("data", (data: Buffer) => {
    try {
      const parsedCommand = parseRESPCommand(data);

      if (!parsedCommand) {
        socket.write(encodeError("ERR Invalid command format"));
        return;
      }

      const {command, args} = parsedCommand;

      switch (command) {
        case "PING":
          socket.write(encodeSimpleString("PONG"));
          break;

        case "ECHO":
          if (args.length !== 1) {
            socket.write(
              encodeError("ERR wrong number of arguments for 'echo' command")
            );
            return;
          }
          socket.write(encodeBulkString(args[0]));
          break;

        case "SET":
          if (args.length === 2) {
            // SET key value (no expiry)
            kvStore.set(args[0], {value: args[1]});
          } else if (args.length === 4 && args[2].toUpperCase() === "PX") {
            // SET key value PX milliseconds
            const px = parseInt(args[3]);
            const expiry = Date.now() + px;
            kvStore.set(args[0], {value: args[1], expiry});
          } else {
            socket.write(encodeError("ERR syntax error"));
            return;
          }
          socket.write(encodeSimpleString("OK"));
          break;
        case "GET":
          const entry = kvStore.get(args[0]);
          if (!entry) {
            socket.write(encodeBulkString(null));
            return;
          }

          if (entry.expiry && Date.now() > entry.expiry) {
            kvStore.delete(args[0]); // Optional cleanup
            socket.write(encodeBulkString(null));
            return;
          }

          socket.write(encodeBulkString(entry.value));
          break;
        default:
          socket.write(encodeError(`ERR unknown command '${command}'`));
          break;
      }
    } catch (error) {
      console.error("Error processing command:", error);
      socket.write(encodeError("ERR Internal server error"));
    }
  });
});

server.listen(6379, "127.0.0.1");
