// Simple in-memory key-value store
const kvStore = new Map<string, string>();

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
          if (args.length !== 2) {
            socket.write(
              encodeError("ERR wrong number of arguments for 'set' command")
            );
            return;
          }
          kvStore.set(args[0], args[1]);
          socket.write(encodeSimpleString("OK"));
          break;

        case "GET":
          if (args.length !== 1) {
            socket.write(
              encodeError("ERR wrong number of arguments for 'get' command")
            );
            return;
          }
          const value = kvStore.get(args[0]);
          if (value === undefined) {
            socket.write(encodeBulkString(null));
          } else {
            socket.write(encodeBulkString(value));
          }
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
